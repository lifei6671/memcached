package memcached

import (
	"net"
	"time"
)

// MemcachedPeer struct.
type MemcachedPeer struct {
	addr net.Addr
	hash uint
	weight int
	pool *MemcachedPool
}


func NewMemcachedPeer(addr net.Addr) *MemcachedPeer {
	peer := &MemcachedPeer{ addr : addr  }

	pool := NewMemcachedPool(func() (*Conn,error){

		nc,err := net.Dial(addr.Network(),addr.String())

		if err != nil {
			return nil,err
		}
		conn := NewConn(nc)
		return conn,nil

	},MAX_POOL_SIZE)

	peer.pool = pool

	return peer
}

// InitPeer 初始化连接池中的连接.
func (c *MemcachedPeer) InitPeer(timeout time.Duration) {

	if c.pool.MaxActiveConnections > 0 {
		for i := 0; i <= c.pool.MaxIdleConnections; i ++ {
			nc, err := net.DialTimeout(c.addr.Network(), c.addr.String(), timeout)
			if err != nil {
				panic(err)
			}

			conn := NewConn(nc)
			c.pool.PutFreeConn(conn)
		}
	}
}