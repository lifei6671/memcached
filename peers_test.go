package memcached

import (
	"testing"
	"time"
)

func TestMemcachedPeer_InitPeer(t *testing.T) {
	client := NewMemcachedClient()
	client.SetTimeout(time.Second * 30)
	err := client.AddServer("127.0.0.1:11211",1)
	if err != nil {
		t.Fatal(err)
	}

}
