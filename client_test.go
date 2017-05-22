package memcached

import (
	"testing"
	"time"
)

func TestMemcachedClient_AddServer(t *testing.T) {
	client := NewMemcachedClient()
	client.SetTimeout(time.Second * 30)

	err := client.AddServer("127.0.0.1:11211",1)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("%+v",client)
}

func TestMemcachedClient_Set(t *testing.T) {
	client := NewMemcachedClient()
	client.SetTimeout(time.Second * 30)

	err := client.AddServer("192.168.3.104:11211",1)
	if err != nil {
		t.Fatal(err)
	}

	err = client.Set("key",[]byte("在发送了该命令行和数据块后，客户端等待可能的响应"),false,time.Second * 3600)
	if err != nil {
		t.Fatal(err)
	}
}

func TestMemcachedClient_Get(t *testing.T) {
	client := NewMemcachedClient()
	client.SetTimeout(time.Second * 30)

	err := client.AddServer("192.168.3.104:11211",1)
	if err != nil {
		t.Fatal(err)
	}
	b,err := client.Get("key")
	if err != nil {
		t.Fatal(err)
	}
	t.Log(string(b))
}