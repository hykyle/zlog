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
	dropType:  0,    // 默认满时等待日志
	async:     true, // 默认启用异步日志
	shardSize: 0,    // 默认用go的核心数
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

// Option 属性选项设置函数
type Option func(*Options)

// DropNone 不丢弃任何日志，等待
func DropNone() Option {
	return func(o *Options) {
		o.dropType = 0
	}
}

// DropOldest 日志缓存满则丢弃旧日志
func DropOldest() Option {
	return func(o *Options) {
		o.dropType = 1
	}
}

// DropNewest 日志缓存满则丢弃新日志
func DropNewest() Option {
	return func(o *Options) {
		o.dropType = 2
	}
}

// WithGID 打印协程ID(默认不打印)
func WithGID() Option {
	return func(o *Options) {
		o.withGID = true
	}
}

// WithSync 使用同步日志，比异步慢近十倍
func WithSync() Option {
	return func(o *Options) {
		o.async = false
	}
}

// BufioSize bufio缓存的大小, 默认1024*8
func BufioSize(bufioSize int) Option {
	return func(o *Options) {
		o.bufioSize = bufioSize
	}
}

// RingSize 每个ring buffer的大小, 默认1024*16
func RingSize(ringSize int) Option {
	return func(o *Options) {
		o.ringSize = uint64(ringSize)
	}
}

// ShardSize ring buffer的分片数、默认go的核数
func ShardSize(shardSize int) Option {
	return func(o *Options) {
		o.shardSize = uint64(shardSize)
	}
}

// LogPath 日志文件路径
func LogPath(logPath string) Option {
	return func(o *Options) {
		o.logPath = logPath
	}
}

// WithFields 所有日志都附带的字段
func WithFields(fields map[string]interface{}) Option {
	return func(o *Options) {
		o.fields = fields
	}
}

// Stdout 日志打印到标准输出(默认不输出到stdout)
func Stdout() Option {
	return func(o *Options) {
		o.stdout = true
	}
}

// DebugLevel debug日志等级
func DebugLevel() Option {
	return func(o *Options) {
		o.level = zap.DebugLevel
	}
}

// InfoLevel info日志等级
func InfoLevel() Option {
	return func(o *Options) {
		o.level = zap.InfoLevel
	}
}

// WarnLevel warn日志等级
func WarnLevel() Option {
	return func(o *Options) {
		o.level = zap.WarnLevel
	}
}

// LogLevel 设置日志等级 debug info warn
func LogLevel(lv string) Option {
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
