package memcached

import (
	"fmt"
	"strconv"
	"testing"
	"net"
)

func TestGetInfo(t *testing.T) {
	ring := NewRing(200)

	nodes := map[net.Addr]int{
		&MemcachedAddr{ ntw: "tcp",str: "192.168.3.103:11211"} : 1,
		&MemcachedAddr{ ntw: "tcp",str: "192.168.3.104:11211"} : 1,
		&MemcachedAddr{ ntw: "tcp",str: "192.168.3.105:11211"} : 1,
		&MemcachedAddr{ ntw: "tcp",str: "192.168.3.106:11211"} : 1,
		&MemcachedAddr{ ntw: "tcp",str: "192.168.3.107:11211"} : 1,
	}

	for k, v := range nodes {
		peer := &MemcachedPeer{ addr: k }
		ring.AddNode(peer, v)
	}

	ring.Bake()

	m := make(map[net.Addr]int)
	for i := 0; i < 1e6; i++ {
		m[ring.Hash("test value"+strconv.FormatUint(uint64(i), 10) ).addr]++
	}

	for k := range nodes {
		fmt.Println(k.String(), m[k])
	}

}
