package zlog

import (
	"io"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

const (
	maxFileMB  = 500 // 单位MB
	maxBackups = 10
	maxAge     = 7
)

// Flusher 刷新/提交
type Flusher interface {
	Flush() error
}

// WriteFlushSyncer 组合接口
type WriteFlushSyncer struct {
	io.Writer
	Flusher
}

// Sync 实现io.Syncer接口，Sync内部调用 Flush
func (b *WriteFlushSyncer) Sync() error {
	return b.Flush()
}

func newAsyncLogger(opt *Options) (*zap.Logger, error) {
	// 日志文件
	filePath := getLogFilePath(&defaultOptions)
	lj := &lumberjack.Logger{
		Filename:   filePath,   // 日志文件路径
		Compress:   true,       // 是否对旧日志文件进行压缩
		LocalTime:  true,       // 是否使用本地时间，否则使用UTC(协调世界时,全世界都公用的一个时间)时间
		MaxSize:    maxFileMB,  // 单个日志文件大小(MB)
		MaxBackups: maxBackups, // 旧日志文件保存的最大数量，默认保存所有
		MaxAge:     maxAge,     // 日志最大存活时长（天）
	}
	ljws := zapcore.AddSync(lj)
	ws := &zapcore.BufferedWriteSyncer{
		WS:            ljws,
		Size:          defaultOptions.bufioSize,
		FlushInterval: 100 * time.Millisecond,
	}
	asyncWS := newAsyncWriteSyncer(ws, opt.shardSize, opt.ringSize, opt.batchSize, opt.dropType)

	// encoder 配置
	encoderCfg := zapcore.EncoderConfig{
		MessageKey:     "msg",
		LevelKey:       "level",
		TimeKey:        "time",
		NameKey:        "name",
		CallerKey:      "caller",
		StacktraceKey:  "stack",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
		EncodeDuration: zapcore.StringDurationEncoder,
		EncodeCaller:   callerEncoder,
		EncodeName:     zapcore.FullNameEncoder,
		EncodeTime:     encodeTimeMs,
	}
	encoder := zapcore.NewJSONEncoder(encoderCfg)

	// 日志level控制
	levelEnabler := zap.LevelEnablerFunc(func(lvl zapcore.Level) bool {
		return lvl >= opt.level
	})

	// 构建zap core
	var core zapcore.Core
	if opt.stdout { // stdout 终端多路输出
		var writers []zapcore.WriteSyncer
		writers = append(writers, asyncWS)
		writers = append(writers, zapcore.Lock(os.Stdout))
		mws := zapcore.NewMultiWriteSyncer(writers...) // 多路输出
		core = zapcore.NewCore(encoder, mws, levelEnabler)
	} else {
		core = zapcore.NewCore(encoder, asyncWS, levelEnabler)
	}

	// 日志函数logger
	opts := []zap.Option{zap.AddCaller(), zap.AddCallerSkip(1)}

	// 全局自带字段
	if len(opt.fields) > 0 {
		fl := make([]zap.Field, 0, len(opt.fields))
		for k, v := range opt.fields {
			fl = append(fl, zap.Any(k, v))
		}
		opts = append(opts, zap.Fields(fl...))
	}

	logger := zap.New(core, opts...)
	return logger, nil
}

// =====================================================
// AsyncWriteSyncer 异步日志写入器，实现 zapcore.WriteSyncer
// =====================================================

// AsyncWriteSyncer 异步缓冲写入，MPSC分片队列+后台批量刷盘
type AsyncWriteSyncer struct {
	ws          zapcore.WriteSyncer
	shardedRing *ShardedRing[[]byte]
	stop        chan struct{}
	wg          sync.WaitGroup
	dropType    DropType
	shardSize   uint64      // 分片数量
	ringSize    uint64      // 队列容量
	batchSize   uint64      // 批量大小
	closed      atomic.Bool // 关闭标记，拦截写入
}

// NewAsyncWriteSyncer 创建异步写入器
// ws: 底层真实写入器(文件/控制台)
// shardSize: ringbuffer的数量
// ringSize: 每个ringbuffer的容量
// batchSize: 每次读ringbuffer的批量
// policy: 队列满丢弃/阻塞策略
func newAsyncWriteSyncer(ws zapcore.WriteSyncer, shardSize, ringSize, batchSize uint64, dropType DropType) *AsyncWriteSyncer {
	a := &AsyncWriteSyncer{
		ws:          ws,
		shardedRing: NewShardedRing[[]byte](shardSize, ringSize),
		stop:        make(chan struct{}),
		dropType:    dropType,
		shardSize:   shardSize,
		ringSize:    ringSize,
		batchSize:   batchSize,
	}
	a.wg.Go(a.run)
	return a
}

// backoff 阻塞写入时退避策略，降低CPU空转
func backoff(spin uint) {
	switch {
	case spin < 8:
		// 纯忙自旋，什么都不做
	case spin < 16:
		// 让出P，G加到runq尾部
		runtime.Gosched()
	default:
		// 定时挂起
		us := 1 << (spin - 16)
		us = min(us, 1000) //延长时间，降低空载CPU
		time.Sleep(time.Duration(us) * time.Microsecond)
	}
}

// Write 实现 zapcore.WriteSyncer 接口，同步入队，异步落地
func (a *AsyncWriteSyncer) Write(p []byte) (int, error) {
	if a.closed.Load() {
		// 已关闭，直接丢弃
		return len(p), nil
	}

	// 拷贝zap pool缓存
	buf := make([]byte, 0, len(p))
	buf = append(buf, p...)
	if !a.shardedRing.PublishG(buf) {
		switch a.dropType {
		case DropNone:
			// 退避等待写日志
			for spin := uint(0); !a.shardedRing.PublishG(buf); spin++ {
				if a.closed.Load() {
					return len(p), nil
				}
				backoff(spin)
			}
		case DropOldest:
			// 删除当前G的一半旧日志
			a.shardedRing.BatchDropG(a.ringSize / 2)
			a.shardedRing.PublishG(buf)
		case DropNewest:
			// 直接丢弃新日志
		}
	}
	return len(p), nil
}

// run 后台消费协程主循环，批量写盘+定时刷盘
func (a *AsyncWriteSyncer) run() {
	spin := uint(0)
	lbs := make([][]byte, 0, a.batchSize)
	for {
		lbs = lbs[:0]
		select {
		case <-a.stop:
			// 排空所有队列数据再退出
			idleCount := uint64(0)
			maxIdle := a.shardedRing.numShards
			for {
				lbs := a.shardedRing.BatchRead(a.batchSize, lbs)
				if len(lbs) == 0 {
					idleCount++
					if idleCount > maxIdle {
						break
					}
					continue
				}
				idleCount = 0

				for _, item := range lbs {
					_, _ = a.ws.Write(item)
				}
			}
			_ = a.ws.Sync()
			return

		default:
			lbs = a.shardedRing.BatchRead(a.batchSize, lbs)
			if len(lbs) == 0 {
				backoff(spin)
				spin++
				continue
			}

			for _, item := range lbs {
				_, _ = a.ws.Write(item)
			}
			spin = 0
		}
	}
}

// Sync 手动同步并关闭日志，阻塞直到队列清空或3s超时
func (a *AsyncWriteSyncer) Sync() error {
	if a.closed.Swap(true) {
		return a.ws.Sync()
	}

	close(a.stop)
	a.wg.Wait()
	return a.ws.Sync()
}

// 确保 AsyncWriteSyncer 实现 zapcore.WriteSyncer 接口
var _ zapcore.WriteSyncer = (*AsyncWriteSyncer)(nil)
