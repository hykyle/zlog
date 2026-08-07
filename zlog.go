// Package zlog 基于zap封装的异步日志库，用ringbuffer先缓存在遍历落盘
package zlog

import (
	"os"
	"sync"

	"github.com/petermattis/goid"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	appInnerLog *zap.Logger
	initOnce    sync.Once
)

// GetLogger  获取 appInnerLog
func GetLogger() *zap.Logger {
	if appInnerLog == nil {
		initOnce.Do(func() { _ = InitLog() })
	}
	return appInnerLog
}

// InitLog 初始化日志
func InitLog(opts ...Option) error {
	_ = closeLog()

	for _, opt := range opts {
		//遍历执行传进来的opts,也就是func(*Options)函数
		opt(&defaultOptions)
	}

	// 默认启用异步日志
	if defaultOptions.async {
		innerLog, err := newAsyncLogger(&defaultOptions)
		if err != nil {
			return err
		}
		appInnerLog = innerLog
	} else {
		innerLog, err := newSyncLogger(&defaultOptions)
		if err != nil {
			return err
		}
		appInnerLog = innerLog
	}
	return nil
}

func closeLog() error {
	//如果日志器为空
	if appInnerLog != nil {
		//将缓存同步到文件
		return appInnerLog.Sync()
	}
	return nil
}

// Debug logs a message at DebugLevel. The message includes any fields passed
// at the log site, as well as any fields accumulated on the logger.
func Debug(msg string, fields ...zapcore.Field) {
	GetLogger().Debug(msg, addGoID(fields)...)
}

// Info logs a message at InfoLevel. The message includes any fields passed
// at the log site, as well as any fields accumulated on the logger.
func Info(msg string, fields ...zapcore.Field) {
	GetLogger().Info(msg, addGoID(fields)...)
}

// Warn logs a message at WarnLevel. The message includes any fields passed
// at the log site, as well as any fields accumulated on the logger.
func Warn(msg string, fields ...zapcore.Field) {
	GetLogger().Warn(msg, addGoID(fields)...)
}

// Error logs a message at ErrorLevel. The message includes any fields passed
// at the log site, as well as any fields accumulated on the logger.
func Error(msg string, fields ...zapcore.Field) {
	GetLogger().Error(msg, addGoID(fields)...)
}

// PanicAsync logs a message at ErrorLevel and flush to file. The message includes any fields passed
// at the log site, as well as any fields accumulated on the logger.
// The logger then closed and panics, even if logging at PanicLevel is disabled.
func PanicAsync(msg string, fields ...zapcore.Field) {
	GetLogger().Error("panic:"+msg, addGoID(fields)...)
	_ = appInnerLog.Sync()
	panic(msg)
}

// FatalAsync logs a message at FatalLevel and flush to file. The message includes any fields passed
// at the log site, as well as any fields accumulated on the logger.
//
// The logger then calls os.Exit(1), even if logging at FatalLevel is disabled.
func FatalAsync(msg string, fields ...zapcore.Field) {
	GetLogger().Error("fatal:"+msg, addGoID(fields)...)
	_ = appInnerLog.Sync()
	os.Exit(1)
}

// DPanic logs a message at DPanicLevel. The message includes any fields passed
// at the log site, as well as any fields accumulated on the logger.
//
// If the logger is in development mode, it then panics (DPanic means
// "development panic"). This is useful for catching errors that are
// recoverable, but shouldn't ever happen.
func DPanic(msg string, fields ...zapcore.Field) {
	GetLogger().DPanic(msg, addGoID(fields)...)
}

// Panic logs a message at PanicLevel. The message includes any fields passed
// at the log site, as well as any fields accumulated on the logger.
//
// The logger then panics, even if logging at PanicLevel is disabled.
func Panic(msg string, fields ...zapcore.Field) {
	GetLogger().Panic(msg, addGoID(fields)...)
}

// Fatal logs a message at FatalLevel. The message includes any fields passed
// at the log site, as well as any fields accumulated on the logger.
//
// The logger then calls os.Exit(1), even if logging at FatalLevel is disabled.
func Fatal(msg string, fields ...zapcore.Field) {
	GetLogger().Fatal(msg, addGoID(fields)...)
}

// Sync flush日志到文件，并关闭日志
func Sync() error {
	if appInnerLog != nil {
		return appInnerLog.Sync()
	}
	return nil
}

// LogLevelEnable returns true if the given level is at or above this level.
func LogLevelEnable(level zapcore.Level) bool {
	return appInnerLog.Core().Enabled(level)
}

func addGoID(fields []zapcore.Field) []zapcore.Field {
	if defaultOptions.withGID {
		return append(fields, GoID(goid.Get()))
	}
	return fields
}
