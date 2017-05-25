package memcached

import (
	"container/list"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type MemcachedPool struct {
	//自定义方法实现连接的生成.
	Dial DialFunc
	//连接池中最大空闲的连接数量.
	MaxIdleConnections int
	//连接池中最大活动的连接数量.
	MaxActiveConnections int

	//空闲指定时间后关闭连接.
	IdleTimeout time.Duration

	//连接存货检查频率.
	IdleFrequency time.Duration
	//锁.
	mux sync.Mutex

	//标识连接池已关闭.
	closed bool
	//当前活动的连接数量.
	currentActiveNumber int32

	idle *list.List
}

type idleConn struct {
	c *Conn
	t time.Time
}
// DialFunc 连接池生成的方法.
type DialFunc func() (*Conn, error)

// NewMemcachedPool 初始化连接池.
func NewMemcachedPool(dialFunc DialFunc, maxIdle int) *MemcachedPool {
	pool := &MemcachedPool{
		Dial:               dialFunc,
		MaxIdleConnections: maxIdle,
		mux:                sync.Mutex{},
		idle:               list.New(),
	}
	go pool.checkIdleConn()

	return pool
}

// PopFreeConn 弹出一个可用的空闲连接.
func (c *MemcachedPool) PopFreeConn() (*Conn, error) {
	c.mux.Lock()

	//如果空闲时间设置的大于0，则进行空闲时间判断
	//if timeout := c.IdleTimeout; timeout > 0 {
	//	for i, n := 0, c.idle.Len(); i < n; i++ {
	//		e := c.idle.Back()
	//		if e == nil {
	//			break
	//		}
	//		ic := e.Value.(idleConn)
	//		if ic.t.Add(timeout).After(time.Now()) {
	//			break
	//		}
	//		c.idle.Remove(e)
	//		c.release()
	//		c.mux.Unlock()
	//		ic.c.Close()
	//		c.mux.Lock()
	//	}
	//}

	for {

		// Get idle connection.

		for i, n := 0, c.idle.Len(); i < n; i++ {
			e := c.idle.Front()
			if e == nil {
				break
			}
			ic := e.Value.(idleConn)
			c.idle.Remove(e)

			c.mux.Unlock()
			ic.c.Close()
			c.mux.Lock()
			c.release()
		}

		// 检查连接池是否已关闭

		if c.closed {
			c.mux.Unlock()
			return nil, ErrPoolClosed
		}

		activeNumber := int(atomic.LoadInt32(&(c.currentActiveNumber)))

		// 检查当前活动的连接是否超过了限定的连接数量.
		if c.MaxActiveConnections == 0 || activeNumber < c.MaxActiveConnections {
			dial := c.Dial
			c.MaxActiveConnections += 1
			c.mux.Unlock()
			p, err := dial()
			if err != nil {
				c.mux.Lock()
				c.release()
				c.mux.Unlock()
				p = nil
			}
			return p, err
		}

		return nil, ErrPoolExhausted
	}
}

// PutFreeConn 将一个连接放入连接池.
func (c *MemcachedPool) PutFreeConn(conn *Conn) error {
	c.mux.Lock()

	//如果连接池没有关闭，并且当前活动的连接没有超过最大活动连接限制.
	if !c.closed {
		conn.SetDeadline(time.Time{})

		c.idle.PushFront(idleConn{c: conn, t: time.Now()})

		if c.idle.Len() > c.MaxIdleConnections {
			conn = c.idle.Remove(c.idle.Back()).(idleConn).c
		} else {
			conn = nil
		}
	}
	//如果为空标识添加到池中成功.
	if conn == nil {
		c.mux.Unlock()
		return nil
	}
	//否则标识移除了队尾的连接，需要释放活动数量，并主动关闭连接.
	c.release()
	c.mux.Unlock()
	return conn.Close()
}

//释放连接数量.
func (c *MemcachedPool) release() {
	atomic.AddInt32(&(c.currentActiveNumber), -1)
}

// Close 关闭连接池并释放所有已存在连接.
func (c *MemcachedPool) Close() error {
	c.mux.Lock()
	idle := c.idle
	c.idle.Init()
	c.closed = true

	atomic.AddInt32(&(c.currentActiveNumber), int32(-idle.Len()))

	c.mux.Unlock()
	for e := idle.Front(); e != nil; e = e.Next() {
		e.Value.(idleConn).c.Close()
	}
	return nil
}

// checkIdleConn 定期检查连接池中的连接是否超过了设定的空闲期限.
func (c *MemcachedPool) checkIdleConn() {
	for c.IdleFrequency <= 0 {
		runtime.Gosched()
	}

	ticker := time.NewTicker(c.IdleFrequency)

	defer ticker.Stop()

	for range ticker.C {
		if c.closed {
			break
		}

		//如果空闲时间设置的大于0，则进行空闲时间判断
		if timeout := c.IdleTimeout; timeout > 0 {
			c.mux.Lock()
			for i, n := 0, c.idle.Len(); i < n; i++ {
				e := c.idle.Back()
				if e == nil {
					break
				}
				ic := e.Value.(idleConn)
				if ic.t.Add(timeout).After(time.Now()) {
					break
				}
				c.idle.Remove(e)
				c.release()
				c.mux.Unlock()
				ic.c.Close()
				c.mux.Lock()
			}
			c.mux.Unlock()
		}
	}
}
