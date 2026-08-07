package zlog

import (
	"io"
	"os"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

// WriteCloseSyncer 组合接口
type WriteCloseSyncer struct {
	io.Writer
	io.Closer
}

// Sync 实现io.Syncer接口，Sync内部调用 Flush
func (b *WriteCloseSyncer) Sync() error {
	return b.Close()
}

// 一步一个痕迹的严苛场景，任何日志不能丢失
func newSyncLogger(opt *Options) (*zap.Logger, error) {
	// 日志文件
	filePath := getLogFilePath(&defaultOptions)
	writer := &lumberjack.Logger{
		Filename:   filePath,   // 日志文件路径
		Compress:   true,       // 是否对旧日志文件进行压缩
		LocalTime:  true,       // 是否使用本地时间，否则使用UTC(协调世界时,全世界都公用的一个时间)时间
		MaxSize:    maxFileMB,  // 单个日志文件大小(MB)
		MaxBackups: maxBackups, // 旧日志文件保存的最大数量，默认保存所有
		MaxAge:     maxAge,     // 日志最大存活时长（天）
	}
	wc := &WriteCloseSyncer{Writer: writer, Closer: writer}

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
		EncodeTime: func(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
			enc.AppendString(t.Format("2006-01-02T15:04:05.000"))
		},
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
		writers = append(writers, wc)
		writers = append(writers, zapcore.Lock(os.Stdout))
		mws := zapcore.NewMultiWriteSyncer(writers...) // 多路输出
		core = zapcore.NewCore(encoder, mws, levelEnabler)
	} else {
		core = zapcore.NewCore(encoder, wc, levelEnabler)
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
