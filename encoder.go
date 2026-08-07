package zlog

import (
	"runtime"
	"strings"
	"time"
	"unsafe"

	"go.uber.org/zap/zapcore"
)

//go:inline
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

// 预计算 00-99 的 ASCII 表示，避免重复除法
var digits100 [100][2]byte

func init() {
	for i := range 100 {
		digits100[i][0] = byte(i/10) + '0'
		digits100[i][1] = byte(i%10) + '0'
	}
}

//go:inline
func encodeTimeMs(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
	var buf [23]byte
	year, month, day := t.Date()
	hour, min, sec := t.Clock()
	ms := t.Nanosecond() / 1_000_000

	// 年份：4位数字，手动展开避免循环
	y := uint(year) // 转 uint 避免负数检查，编译器可更好优化
	buf[0] = byte(y/1000) + '0'
	buf[1] = byte((y/100)%10) + '0'
	buf[2] = byte((y/10)%10) + '0'
	buf[3] = byte(y%10) + '0'
	buf[4] = '-'

	// 月、日、时、分、秒 使用查找表
	p := digits100[month]
	buf[5], buf[6] = p[0], p[1]
	buf[7] = '-'

	p = digits100[day]
	buf[8], buf[9] = p[0], p[1]
	buf[10] = 'T'

	p = digits100[hour]
	buf[11], buf[12] = p[0], p[1]
	buf[13] = ':'

	p = digits100[min]
	buf[14], buf[15] = p[0], p[1]
	buf[16] = ':'

	p = digits100[sec]
	buf[17], buf[18] = p[0], p[1]
	buf[19] = '.'

	// 毫秒：3位数字
	buf[20] = byte(ms/100) + '0'
	buf[21] = byte((ms/10)%10) + '0'
	buf[22] = byte(ms%10) + '0'

	s := unsafe.String(&buf[0], 23)
	enc.AppendString(s)
}
