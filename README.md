# zap 日志库的异步文件日志

## 1、使用方式

需要执行InitLog函数初始化

```go
zlog.InitLog()
defer zlog.Sync()
```

### 2、同步日志

```Go
zlog.InitLog(zlog.OptLogPath("./log/newlog.log"), zlog.OptSync())
```

### 3、异步日志

* 通过gid映射切片只能保证协程内日志有序

```Go
zlog.InitLog(zlog.OptLogPath("./log/newlog.log"))
```

* 若要所有协程都要按照打印日志顺序，则不能使用多分片模式，则

```Go
zlog.InitLog(zlog.OptLogPath("./log/newlog.log",zlog.OptShardSize(1))
```

* 异步日志队列满时，默认直接丢弃新日志。若要保留所有日志，则要设置属性`zlog.OptDropNone()`

```Go
zlog.InitLog(zlog.OptLogPath("./log/newlog.log",zlog.OptDropNone())
```
