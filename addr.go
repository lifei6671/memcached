package memcached

import "net"

type MemcachedAddr struct {
	ntw string
	str string
}

func NewMemcachedAddr(addr net.Addr) net.Addr {
	return &MemcachedAddr{ ntw: addr.Network(), str : addr.String()}
}

func (c *MemcachedAddr) Network() string {
	return c.ntw
}

func (c *MemcachedAddr) String() string {
	return c.str
}

func (c *MemcachedAddr) Equals(addr *MemcachedAddr) bool {
	return c.ntw == addr.ntw && c.str == addr.str
}