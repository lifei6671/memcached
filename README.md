# memcached

Golang实现的Memcached客户端连接库。内部通过一致性hash算法维护缓存的分布。主要功能如下：

- 支持连接池
- 支持连接过期回收
- 支持故障检查和恢复


## 安装

使用 `go get` 命令安装：

```bash
go get github.com/lifei6671/memcached
```

该库没有其他第三方依赖。

## 使用

使用如下：

```
client := NewMemcachedClient()
client.SetTimeout(time.Second * 30)

err := client.AddServer("192.168.3.104:11211",15)
if err != nil {
	log.Fatal(err)
}
client.AddServer("192.168.3.103:11211",1)
client.AddServer("192.168.3.105:11211",1)
client.AddServer("192.168.3.106:11211",1)
client.Init()

b,err := client.Get("ReceiveTimeout")
if err != nil {
	log.Fatalf("Error : %s,Result : %+v",err,b)
}
```

更多文档请访问：[https://godoc.org/github.com/lifei6671/memcached](https://godoc.org/github.com/lifei6671/memcached)
