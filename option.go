package zlog

import (
	"os"
	"path/filepath"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

//属性设置全部以func(*Options)函数类型返回的形式传递，到达初始化函数再一个个遍历执行

// Options 属性
type Options struct {
	level     zapcore.Level  // 测试环境日志级别为debug
	logPath   string         // 日志路径
	async     bool           // 异步日志
	withGID   bool           // 打印协程id
	stdout    bool           // 日志同时打印到标准输出
	dropType  int            // 日志缓存满则丢弃新日志
	shardSize uint64         // ring buffer的分片数量
	ringSize  uint64         // 每个ring buffer的容量
	batchSize uint64         // 每次读ring buffer的批量
	bufioSize int            // 写文件bufio的缓存大小
	fields    map[string]any // 日志默认附加的字段
}

var defaultOptions = Options{
	level:     zap.DebugLevel,
	withGID:   false,
	stdout:    false,
	dropType:  DropNewest, // 默认满时丢弃新日志
	async:     true,       // 默认启用异步日志
	shardSize: 0,          // 默认用go的核心数
	batchSize: 256,
	ringSize:  1024 * 16,
	bufioSize: 1024 * 64,
}

func getLogFilePath(opt *Options) string {
	if len(opt.logPath) == 0 {
		return filepath.Join("log", processName(), processName()+".log")
	}
	return opt.logPath
}

// 提取进程名
func processName() string {
	base := filepath.Base(os.Args[0])
	name := strings.TrimSuffix(base, filepath.Ext(base))
	return name
}

// DropType 日志溢出的丢弃策略
type DropType = int

const (
	// DropNone 不丢弃日志
	DropNone DropType = 0

	// DropOldest 丢弃旧日志
	DropOldest DropType = 1

	// DropNewest 丢弃新日志
	DropNewest DropType = 2
)

// Option 属性选项设置函数
type Option func(*Options)

// OptDropNone 不丢弃任何日志，等待
func OptDropNone() Option {
	return func(o *Options) {
		o.dropType = DropNone
	}
}

// OptDropOldest 日志缓存满则丢弃旧日志
func OptDropOldest() Option {
	return func(o *Options) {
		o.dropType = DropOldest
	}
}

// OptDropNewest 日志缓存满则丢弃新日志
func OptDropNewest() Option {
	return func(o *Options) {
		o.dropType = DropNewest
	}
}

// OptWithGID 打印协程ID(默认不打印)
func OptWithGID() Option {
	return func(o *Options) {
		o.withGID = true
	}
}

// OptSync 使用同步日志，比异步慢近十倍
func OptSync() Option {
	return func(o *Options) {
		o.async = false
	}
}

// OptBufioSize bufio缓存的大小, 默认1024*8
func OptBufioSize(bufioSize int) Option {
	return func(o *Options) {
		o.bufioSize = bufioSize
	}
}

// OptRingSize 每个ring buffer的大小, 默认1024*16
func OptRingSize(ringSize int) Option {
	return func(o *Options) {
		o.ringSize = uint64(ringSize)
	}
}

// OptShardSize ring buffer的分片数、默认go的核数
func OptShardSize(shardSize int) Option {
	return func(o *Options) {
		o.shardSize = uint64(shardSize)
	}
}

// OptLogPath 日志文件路径
func OptLogPath(logPath string) Option {
	return func(o *Options) {
		o.logPath = logPath
	}
}

// OptWithFields 所有日志都附带的字段
func OptWithFields(fields map[string]interface{}) Option {
	return func(o *Options) {
		o.fields = fields
	}
}

// OptStdout 日志打印到标准输出(默认不输出到stdout)
func OptStdout() Option {
	return func(o *Options) {
		o.stdout = true
	}
}

// OptDebugLevel debug日志等级
func OptDebugLevel() Option {
	return func(o *Options) {
		o.level = zap.DebugLevel
	}
}

// OptInfoLevel info日志等级
func OptInfoLevel() Option {
	return func(o *Options) {
		o.level = zap.InfoLevel
	}
}

// OptWarnLevel warn日志等级
func OptWarnLevel() Option {
	return func(o *Options) {
		o.level = zap.WarnLevel
	}
}

// OptLogLevel 设置日志等级 debug info warn
func OptLogLevel(lv string) Option {
	return func(o *Options) {
		lv = strings.ToLower(lv)
		switch lv {
		case "debug":
			o.level = zap.DebugLevel
		case "info":
			o.level = zap.InfoLevel
		case "warn":
			o.level = zap.WarnLevel
		default:
			o.level = zap.InfoLevel
		}
	}
}
