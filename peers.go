package memcached

import (
	"net"
	"time"
)


type MemcachedPeer struct {
	max int
	min int
	addr net.Addr
	hash uint
	//服务器连接失败时重试的间隔时间，默认值15秒。如果此参数设置为-1表示不重试.
	retryInterval int
	pool chan *Conn
}

func NewMemcachedPeer(addr net.Addr) *MemcachedPeer {
	peer := &MemcachedPeer{  addr : addr , retryInterval: 15}

	return peer
}

func (c *MemcachedPeer) Freed(conn *Conn) {
	if c.pool == nil {
		c.pool = make(chan *Conn,c.max)
	}
	if len(c.pool) > c.max {
		conn.Close()
		return
	}
	c.pool <- conn
}

func (c *MemcachedPeer) getFreeConn(timeout time.Duration) *Conn {

	if len(c.pool) <= 0 {
		nc,err := net.DialTimeout(c.addr.Network(),c.addr.String(), timeout)
		if err != nil {
			if c.retryInterval == -1 {
				panic(err)
			}
			interval := 3
			for interval > 0 {
				time.Sleep(time.Second * 15)
				nc,err = net.DialTimeout(c.addr.Network(),c.addr.String(), timeout)
				if err != nil {
					continue
				}
				interval --
			}
			if err != nil {
				panic(err)
			}
		}
		conn := NewConn(nc)

		c.Freed(conn)
	}

	return <- c.pool
}


func (c *MemcachedPeer) InitPeer(maxConnection, minConnection int,timeout time.Duration) net.Conn {
	if maxConnection < minConnection {
		panic("The maximum number of connections can not be less than the minimum number of connections.")
	}
	c.max = maxConnection
	c.min = minConnection
	if c.max > 0 {
		c.pool = make(chan *Conn,c.max)
	}
	go func() {
		if c.min > 0 {
			for i := 0; i < c.min ;i ++ {
				nc,err := net.DialTimeout(c.addr.Network(),c.addr.String(),timeout)
				if err != nil {
					panic(err)
				}

				conn := NewConn(nc)
				c.pool <- conn
			}
		}
	}()

	return <- c.pool
}