# zap 日志库的异步文件日志

## 使用方式

需要执行InitLog函数初始化

```go
zlog.InitLog()
defer zlog.Sync()
```

### 同步日志

```Go
zlog.InitLog(zlog.LogPath("./log/newlog.log"), zlog.WithSync())
```

### 异步日志

```Go
zlog.InitLog(zlog.LogPath("./log/newlog.log"))
```
