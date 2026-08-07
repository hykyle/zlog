package zlog

import (
	"runtime"
	"strings"
	"time"

	"go.uber.org/zap/zapcore"
)

// ext 获取函数名
func ext(name string) string {
	idx := strings.LastIndexByte(name, '.')
	if idx == -1 {
		return ""
	}
	return name[idx+1:]
}

// callerEncoder format: "filename:funcName", e.g:"zaplog/zaplog_test.go:zaplog.TestNewLogger"
func callerEncoder(caller zapcore.EntryCaller, enc zapcore.PrimitiveArrayEncoder) {
	path := caller.TrimmedPath()
	funcName := ext(runtime.FuncForPC(caller.PC).Name())

	total := len(path) + 1 + len(funcName)
	buf := make([]byte, 0, total)
	buf = append(buf, path...)
	buf = append(buf, ':')
	buf = append(buf, funcName...)

	enc.AppendString(string(buf))
}

//go:inline
func encodeTimeMs(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
	var buf [23]byte
	year, month, day := t.Date()
	hour, min, sec := t.Clock()
	ms := t.Nanosecond() / 1_000_000

	buf[0] = byte(year/1000) + '0'
	buf[1] = byte((year/100)%10) + '0'
	buf[2] = byte((year/10)%10) + '0'
	buf[3] = byte(year%10) + '0'
	buf[4] = '-'

	buf[5] = byte(month/10) + '0'
	buf[6] = byte(month%10) + '0'
	buf[7] = '-'

	buf[8] = byte(day/10) + '0'
	buf[9] = byte(day%10) + '0'
	buf[10] = 'T'

	buf[11] = byte(hour/10) + '0'
	buf[12] = byte(hour%10) + '0'
	buf[13] = ':'

	buf[14] = byte(min/10) + '0'
	buf[15] = byte(min%10) + '0'
	buf[16] = ':'

	buf[17] = byte(sec/10) + '0'
	buf[18] = byte(sec%10) + '0'
	buf[19] = '.'

	buf[20] = byte(ms/100) + '0'
	buf[21] = byte((ms/10)%10) + '0'
	buf[22] = byte(ms%10) + '0'

	enc.AppendString(string(buf[:]))
}
