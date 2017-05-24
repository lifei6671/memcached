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

	err = client.Set("ReceiveTimeout",[]byte("在发送了该命令行和数据块后，客户端等\r\n待可能的响应"),50,time.Second * 3600)
	if err != nil {
		t.Fatal(err)
	}
}

func TestMemcachedClient_Add(t *testing.T) {
	client := NewMemcachedClient()
	client.SetTimeout(time.Second * 30)

	err := client.AddServer("192.168.3.104:11211",1)
	if err != nil {
		t.Fatal(err)
	}

	err = client.Add("Sscanf",[]byte("在发送了该命令行和数据块后，客户端等\r\n待可能的响应"),50,time.Second * 3600)
	if err != nil {
		t.Fatal(err)
	}
}


func TestMemcachedClient_Replace(t *testing.T) {
	client := NewMemcachedClient()
	client.SetTimeout(time.Second * 30)

	err := client.AddServer("192.168.3.104:11211",1)
	if err != nil {
		t.Fatal(err)
	}

	err = client.Add("Sscanf",[]byte("replace意思是 “储存此数据，只在服务器曾保留此键值的数据时”"),50,time.Second * 3600)
	if err != nil {
		t.Fatal(err)
	}
}

func TestMemcachedClient_Get(t *testing.T) {
	client := NewMemcachedClient()
	client.SetTimeout(time.Second * 30)

	err := client.AddServer("192.168.3.104:11211",15)
	if err != nil {
		t.Fatal(err)
	}
	client.AddServer("192.168.3.103:11211",1)
	client.AddServer("192.168.3.105:11211",1)
	client.AddServer("192.168.3.106:11211",1)
	b,err := client.Get("ReceiveTimeout")
	if err != nil {
		t.Fatalf("Error : %s,Result : %+v",err,b)
	}
	t.Logf("Value:%+v,Flags:%d",string(b.Value),b.Flags)
	end := make(chan string)
	<- end
}


func TestMemcachedClient_Delete(t *testing.T) {
	client := NewMemcachedClient()
	client.SetTimeout(time.Second * 30)

	err := client.AddServer("192.168.3.104:11211",1)
	if err != nil {
		t.Fatal(err)
	}

	err = client.Delete("Sscanf")
	if err != nil {
		t.Fatal(err)
	}
}

func TestMemcachedClient_Increment(t *testing.T) {
	client := NewMemcachedClient()
	client.SetTimeout(time.Second * 30)

	err := client.AddServer("192.168.3.104:11211",1)
	if err != nil {
		t.Fatal(err)
	}
	client.Set("TestMemcachedClient_Increment",[]byte("0"),10,time.Second)

	v,err := client.Increment("TestMemcachedClient_Increment",10)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(v)
}

func TestMemcachedClient_Stats(t *testing.T) {
	client := NewMemcachedClient()
	client.SetTimeout(time.Second * 30)

	err := client.AddServer("192.168.3.104:11211",1)
	if err != nil {
		t.Fatal(err)
	}

	v,err := client.Stats("192.168.3.104:11211")
	if err != nil {
		t.Fatal(err)
	}
	for _,item := range v {
		t.Logf("%s=%s",item.Name,item.Value)
	}

}

func TestMemcachedClient_Close(t *testing.T) {
	client := NewMemcachedClient()
	client.SetTimeout(time.Second * 30)

	err := client.AddServer("192.168.3.104:11211",1)
	if err != nil {
		t.Fatal(err)
	}

	client.Close()
}

func TestMemcachedClient_Cas(t *testing.T) {
	client := NewMemcachedClient()
	client.SetTimeout(time.Second * 30)

	err := client.AddServer("192.168.3.104:11211",1)

	if err != nil {
		t.Fatal(err)
	}

	err = client.Cas("Sscanf",[]byte("replace意思是 “储存此数据，只在服务器曾保留此键值的数据时”"),50,5,time.Second * 3600)
	if err != nil {
		t.Fatal(err)
	}
}














