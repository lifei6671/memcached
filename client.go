package memcached

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"runtime"
	"strconv"
	"sync"
	"time"
	"net"
)

//https://github.com/go-redis/redis/blob/master/internal/pool/pool.go

const MAX_POOL_SIZE = 2000
const MIN_POOL_SIZE = 5

//默认的虚拟节点数量
const VIRTUAL_SIZE = 500

var (
	crlf            = []byte("\r\n")
	space           = []byte(" ")
	resultValue     = []byte("VALUE")
	resultOK        = []byte("OK\r\n")
	resultStored    = []byte("STORED\r\n")
	resultNotStored = []byte("NOT_STORED\r\n")
	resultNotFound  = []byte("NOT_FOUND\r\n")
	resultDeleted   = []byte("DELETED\r\n")
	resultEnd       = []byte("END\r\n")
	resultExistEnd  = []byte("EXIST\r\n")
	resultError     = []byte("ERROR\r\n")

	resultClientErrorPrefix = []byte("CLIENT_ERROR ")
	resultServerErrorPrefix = []byte("SERVER_ERROR ")
)

//MemcachedClient struct.
type MemcachedClient struct {
	mux sync.RWMutex
	//服务选择算法.
	ring *HashRing
	//服务映射.
	peers map[string]*MemcachedPeer
	//故障服务列表.
	malfunction map[string]*MemcachedPeer
	//读超时时间.
	readTimeout time.Duration
	//写超时时间.
	writeTimeout time.Duration
	//连接超时时间.
	timeout time.Duration

	enableCompression bool
	//服务器检查时间间隔.
	peerFrequency time.Duration
	//心跳检查时间间隔.
	idleFrequency        time.Duration
	maxActiveConnections int
	maxIdleConnections   int
	idleTimeout          time.Duration
}

func NewMemcachedClient(servers ...string) *MemcachedClient {
	addrs := make(map[string]*MemcachedPeer, len(servers))
	ring := NewHashRing(VIRTUAL_SIZE)

	for _, server := range servers {
		addr, err := ResolveMemcachedAddr(server)

		if err != nil {
			panic(err.Error())
		}
		peer := NewMemcachedPeer(addr)
		peer.pool.MaxIdleConnections = MAX_POOL_SIZE
		peer.pool.MaxActiveConnections = MIN_POOL_SIZE
		peer.weight = 1
		addrs[addr.String()] = peer

		ring.AddNode(addr.String(), 1)
	}
	ring.Generate()


	client := &MemcachedClient{
		mux:               sync.RWMutex{},
		peers:             addrs,
		malfunction: 	   make(map[string]*MemcachedPeer, 0),
		ring:              ring,
		readTimeout:       time.Second * 30,
		writeTimeout:      time.Second * 30,
		timeout:           time.Second * 30,
		peerFrequency:	   time.Second * 30,
		enableCompression: false,
	}

	go client.checkPeer()

	return client
}

//新增一个 Memcached 服务器.
func (c *MemcachedClient) AddServer(server string, weight int) error {
	addr, err := ResolveMemcachedAddr(server)

	if err != nil {
		return err
	}

	c.mux.Lock()
	defer c.mux.Unlock()

	peer := NewMemcachedPeer(addr)
	peer.pool.MaxIdleConnections = c.maxIdleConnections
	peer.pool.MaxActiveConnections = c.maxActiveConnections
	peer.pool.IdleTimeout = c.idleTimeout
	peer.pool.IdleFrequency = c.idleFrequency
	peer.weight = weight

	c.peers[peer.addr.String()] = peer

	c.ring.AddNode(peer.addr.String(), weight).Generate()

	return nil
}

//设置读超时时间.
func (c *MemcachedClient) SetReadTimeout(readTimeout time.Duration) {
	c.readTimeout = readTimeout
}

//设置写超时时间.
func (c *MemcachedClient) SetWriteTimeout(writeTimeout time.Duration) {
	c.writeTimeout = writeTimeout
}

//设置连接池中最大空闲的连接数量.
func (c *MemcachedClient) SetMaxIdleConnection(n int) {
	c.maxIdleConnections = n
}

//设置最大活动连接数.
func (c *MemcachedClient) SetMaxActiveConnection(n int) {
	c.maxActiveConnections = n
}

//设置连接休眠时间检查间隔.
func (c *MemcachedClient) SetIdleFrequency(frequency time.Duration) {
	c.idleFrequency = frequency
}

//设置连接超时.
func (c *MemcachedClient) SetTimeout(timeout time.Duration) {
	c.timeout = timeout
}

//设置连接空闲时间间隔.
func (c *MemcachedClient) SetIdleTimeout(timeout time.Duration) {
	c.idleTimeout = timeout
}

func (c *MemcachedClient) SetServerFrequency(timeout time.Duration) {
	c.peerFrequency = timeout
}

//使配置生效，如果变更了配置，需要调用该方法才能使已初始化的连接生效.
func (c *MemcachedClient) Init() {
	c.mux.Lock()
	defer c.mux.Unlock()

	for _, peer := range c.peers {
		peer.pool.IdleFrequency = c.idleFrequency
		peer.pool.IdleTimeout = c.idleTimeout
		peer.pool.MaxActiveConnections = c.maxActiveConnections
		peer.pool.MaxIdleConnections = c.maxIdleConnections
	}
}

//是否启用压缩.
//func (c *MemcachedClient) EnableCompression (ec bool)  {
//	c.enableCompression = ec
//}

//初始化连接池.
func (c *MemcachedClient) InitMemcachedClient() {

	if len(c.peers) > 0 {
		c.mux.Lock()
		defer c.mux.Unlock()

		for _, peer := range c.peers {
			go func(peer *MemcachedPeer) {
				peer.InitPeer(c.timeout)
			}(peer)
		}
	}
}

//关闭所有服务的连接，并清空连接池.
func (c *MemcachedClient) Close() {
	c.mux.Lock()
	defer c.mux.Unlock()

	end := &sync.WaitGroup{}

	for _, peer := range c.peers {
		end.Add(1)
		go func(peer *MemcachedPeer, end *sync.WaitGroup) {
			defer end.Done()
			peer.pool.Close()
		}(peer, end)
	}

	end.Wait()
}

//设置.
func (c *MemcachedClient) Set(key string, v []byte, flag uint16, expire time.Duration) error {

	if !checkKey(key) {
		return ErrKeyTooLong
	}
	entry := &Entry{
		Key:        key,
		Flags:      uint16(flag),
		Expiration: int32(expire.Seconds()),
		Value:      v,
	}

	return c.populateAction(entry, "set")
}

//增加.
func (c *MemcachedClient) Add(key string, v []byte, flag uint16, expire time.Duration) error {
	if !checkKey(key) {
		return ErrKeyTooLong
	}
	entry := &Entry{
		Key:        key,
		Flags:      uint16(flag),
		Expiration: int32(expire.Seconds()),
		Value:      v,
	}

	return c.populateAction(entry, "add")
}

//替换.
func (c *MemcachedClient) Replace(key string, v []byte, flag uint16, expire time.Duration) error {
	if !checkKey(key) {
		return ErrKeyTooLong
	}
	entry := &Entry{
		Key:        key,
		Flags:      uint16(flag),
		Expiration: int32(expire.Seconds()),
		Value:      v,
	}

	return c.populateAction(entry, "replace")
}

//获取指定键的值.
func (c *MemcachedClient) Get(key string) (*Entry, error) {
	peer := c.pickServer(key)
	if peer != nil {

		conn, err := peer.pool.PopFreeConn()
		if err != nil {
			return nil, err
		}
		defer peer.pool.PutFreeConn(conn)

		_, err = fmt.Fprintf(conn, "%s %s\r\n", "gets", key)
		if err != nil {
			log.Println(err)
			return nil, err
		}
		buf := bytes.NewBufferString("")

		for {
			b := make([]byte, 2048)

			n, err := conn.Read(b)
			if err != nil {
				return nil, err
			}
			buf.Write(b[:n])

			if err := c.checkCommomError(buf.Bytes()); err != nil {
				return nil, err
			}
			//如果是END
			if bytes.HasSuffix(buf.Bytes(), resultEnd) {

				items, err := c.resolveGetValue(buf)

				if err != nil {
					log.Println("resolveGetValue => ", err)
					return nil, err
				}

				return items[0], nil
			}
		}

	}
	return nil, ErrNotServer
}

//删除缓存.
func (c *MemcachedClient) Delete(key string) error {
	peer := c.pickServer(key)
	if peer != nil {
		conn, err := peer.pool.PopFreeConn()
		if err != nil {
			return err
		}
		defer peer.pool.PutFreeConn(conn)

		conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
		n, err := fmt.Fprintf(conn, "%s %s %d\r\n", "delete", key, 0)

		if err != nil {
			log.Printf("delete => %d %s", n, err.Error())
			return err
		}

		buf := bytes.NewBufferString("")

		for {
			b := make([]byte, 2048)
			conn.SetReadDeadline(time.Now().Add(c.readTimeout))
			n, err := conn.Read(b)

			if err != nil {
				log.Println(err.Error())
				return err
			}
			buf.Write(b[:n])
			if err := c.checkCommomError(buf.Bytes()); err != nil {
				return err
			}
			if bytes.Equal(buf.Bytes(), resultDeleted) {
				return nil
			}
			if bytes.Equal(buf.Bytes(), resultNotFound) {
				return ErrCacheNotFound
			}
		}
	}

	return ErrNotServer
}

func (c *MemcachedClient) Increment(key string, delta uint64) (uint64, error) {
	peer := c.pickServer(key)

	if peer != nil {
		conn, err := peer.pool.PopFreeConn()
		if err != nil {
			return 0, err
		}
		defer peer.pool.PutFreeConn(conn)

		conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
		n, err := fmt.Fprintf(conn, "%s %s %d\r\n", "incr", key, delta)

		if err != nil {
			log.Printf("incr => %d %s", n, err.Error())
			return 0, err
		}

		buf := bytes.NewBufferString("")

		for {
			b := make([]byte, 2048)
			conn.SetReadDeadline(time.Now().Add(c.readTimeout))
			n, err := conn.Read(b)

			if err != nil {
				log.Println(err.Error())
				return 0, err
			}
			buf.Write(b[:n])
			if err := c.checkCommomError(buf.Bytes()); err != nil {
				return 0, err
			}
			if bytes.Equal(buf.Bytes(), resultNotFound) {

				return 0, ErrCacheNotFound
			}
			if bytes.HasSuffix(buf.Bytes(), crlf) {
				v := buf.Bytes()[:n-2]
				value, err := strconv.Atoi(string(v))
				return uint64(value), err
			}
		}
	}

	return 0, ErrNotServer
}

func (c *MemcachedClient) Decrement(key string, delta uint64) (uint64, error) {
	peer := c.pickServer(key)

	if peer != nil {
		conn, err := peer.pool.PopFreeConn()
		if err != nil {
			return 0, err
		}
		defer peer.pool.PutFreeConn(conn)

		conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
		n, err := fmt.Fprintf(conn, "%s %s %d\r\n", "decr", key, delta)

		if err != nil {
			log.Printf("decr => %d %s", n, err.Error())
			return 0, err
		}

		buf := bytes.NewBufferString("")

		for {
			b := make([]byte, 2048)
			conn.SetReadDeadline(time.Now().Add(c.readTimeout))
			n, err := conn.Read(b)

			if err != nil {
				log.Println(err.Error())
				return 0, err
			}
			buf.Write(b[:n])
			if err := c.checkCommomError(buf.Bytes()); err != nil {
				return 0, err
			}
			if bytes.Equal(buf.Bytes(), resultNotFound) {

				return 0, ErrCacheNotFound
			}
			if bytes.HasSuffix(buf.Bytes(), crlf) {
				v := buf.Bytes()[:n-2]
				value, err := strconv.Atoi(string(v))
				return uint64(value), err
			}
		}
	}

	return 0, ErrNotServer
}

func (c *MemcachedClient) Cas(key string, v []byte, flag uint16, casid uint64, expire time.Duration) error {
	if !checkKey(key) {
		return ErrKeyTooLong
	}
	entry := &Entry{
		Key:        key,
		Flags:      uint16(flag),
		Expiration: int32(expire.Seconds()),
		Value:      v,
		CasId:      casid,
	}

	return c.populateAction(entry, "cas")
}

type MemcachedStateItem struct {
	Name  string
	Value string
}

//获取指定 Memcached 的状态.
func (c *MemcachedClient) Stats(addr string, args ...string) ([]*MemcachedStateItem, error) {

	c.mux.RLock()
	defer c.mux.RUnlock()

	peer := c.peers[addr]

	if peer != nil {
		conn, err := peer.pool.PopFreeConn()
		if err != nil {
			return nil, err
		}
		defer peer.pool.PutFreeConn(conn)

		conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
		if len(args) > 0 {
			n, err := fmt.Fprintf(conn, "%s %s\r\n", "stats", args[0])

			if err != nil {
				log.Printf("stats => %d %s", n, err.Error())
				return nil, err
			}
		} else {
			n, err := fmt.Fprintf(conn, "%s\r\n", "stats")

			if err != nil {
				log.Printf("stats => %d %s", n, err.Error())
				return nil, err
			}
		}

		buf := bytes.NewBufferString("")

		for {
			b := make([]byte, 2048)
			conn.SetReadDeadline(time.Now().Add(c.readTimeout))
			n, err := conn.Read(b)

			if err != nil {
				log.Println(err.Error())
				return nil, err
			}
			buf.Write(b[:n])
			if err := c.checkCommomError(buf.Bytes()); err != nil {
				return nil, err
			}
			if bytes.HasSuffix(buf.Bytes(), resultEnd) {
				items := make([]*MemcachedStateItem, 0)

				for {
					line, err := buf.ReadBytes('\n')
					if err != nil {
						return nil, err
					}
					if bytes.HasPrefix(line, resultEnd) {
						break
					}
					if bytes.HasPrefix(line, []byte("STAT")) {
						item := &MemcachedStateItem{}
						splits := bytes.Split(line, space)
						if len(splits) >= 3 {
							item.Name = string(splits[1])
							item.Value = string(splits[2])
							items = append(items, item)
						}
					}
				}
				return items, nil
			}
		}
	}

	return nil, ErrNotServer
}

func (c *MemcachedClient) populateAction(entry *Entry, action string) error {

	peer := c.pickServer(entry.Key)
	if peer != nil {
		conn, err := peer.pool.PopFreeConn()
		if err != nil {
			return err
		}
		defer peer.pool.PutFreeConn(conn)

		conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
		if action == "cas" {
			n, err := fmt.Fprintf(conn, "%s %s %d %d %d %d\r\n", action, entry.Key, entry.Flags, entry.Expiration, len(entry.Value), entry.CasId)

			if err != nil {
				log.Printf("%s => %d %s", entry.Key, n, err.Error())
				return err
			}
		} else {
			n, err := fmt.Fprintf(conn, "%s %s %d %d %d\r\n", action, entry.Key, entry.Flags, entry.Expiration, len(entry.Value))

			if err != nil {
				log.Printf("%s => %d %s", entry.Key, n, err.Error())
				return err
			}
		}
		conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
		n, err := conn.Write(entry.Value)

		if err != nil {
			log.Printf("%s => %d %s", entry.Key, n, err.Error())
			return err
		}

		conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
		n, err = conn.Write(crlf)

		if err != nil {
			log.Printf("%s => %d %s", entry.Key, n, err.Error())
			return err
		}
		buf := bytes.NewBufferString("")

		for {
			b := make([]byte, 2048)
			conn.SetReadDeadline(time.Now().Add(c.readTimeout))
			n, err := conn.Read(b)

			if err != nil {
				log.Println(err.Error())
				return err
			}
			buf.Write(b[:n])
			if err := c.checkCommomError(buf.Bytes()); err != nil {
				return err
			}
			if bytes.Equal(buf.Bytes(), resultStored) {
				return nil
			}
			if bytes.Equal(buf.Bytes(), resultNotStored) {
				return ErrNotStored
			}
			if bytes.Equal(buf.Bytes(), resultExistEnd) {
				return ErrExistError
			}
			if bytes.Equal(buf.Bytes(), resultNotFound) {
				return ErrCacheNotFound
			}
			log.Println(buf.String())
		}
	}

	return ErrNotServer
}

func (c *MemcachedClient) checkCommomError(b []byte) error {

	if bytes.Equal(b, resultError) {
		log.Println("ssssssssssssa")
		return ErrUnrecognizedCommand
	}
	if bytes.HasPrefix(b, resultClientErrorPrefix) {
		log.Println("bbbbb")
		body := bytes.TrimPrefix(b, resultClientErrorPrefix)
		body = bytes.TrimSuffix(body, crlf)

		return errors.New(string(body))
	}
	if bytes.HasPrefix(b, resultServerErrorPrefix) {
		log.Println("ssssssssssssa")
		body := bytes.TrimPrefix(b, resultServerErrorPrefix)
		body = bytes.TrimSuffix(body, crlf)

		return errors.New(string(body))
	}
	return nil
}

func (c *MemcachedClient) resolveGetValue(buf *bytes.Buffer) ([]*Entry, error) {

	list := make([]*Entry, 0)

	for {
		line, err := buf.ReadBytes('\r')
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Println("ReadBytes => ", err)
			return nil, err
		}
		if bytes.HasPrefix(line, resultEnd) {
			return list, nil
		}
		if bytes.HasPrefix(line, resultValue) {
			values := bytes.Split(line[:len(line)-2], space)

			if len(values) < 4 {
				continue
			}
			entry := &Entry{}

			entry.Key = string(values[1])
			flags, _ := strconv.Atoi(string(values[2]))
			entry.Flags = uint16(flags)
			if len(values) == 5 {
				caseid, _ := strconv.ParseFloat(string(values[4]), 10)
				entry.CasId = uint64(caseid)
			}
			clen, _ := strconv.Atoi(string(values[3]))
			if clen > 0 {
				buf.ReadByte()
				temp := make([]byte, clen+2)
				n, err := buf.Read(temp)
				if err != nil {
					return nil, err
				}

				if n != clen+2 || !bytes.Equal(temp[clen:], crlf) {
					return nil, ErrResultFormatError
				}
				entry.Value = temp[:clen]
			}
			list = append(list, entry)

		} else {
			continue
		}

	}
	if len(list) <= 0 {
		return nil, ErrCacheNotFound
	}
	return list, nil
}

func (c *MemcachedClient) pickServer(key string) *MemcachedPeer {
	c.mux.RLock()
	defer c.mux.RUnlock()

	return c.peers[c.ring.GetNode(key)]
}

//检查服务状态.
func (c *MemcachedClient) checkPeer() {
	//如果没有设置检查并且也没有可检查的服务器
	for c.peerFrequency <= 0 || (len(c.peers) <= 0 && len(c.malfunction) <= 0){
		runtime.Gosched()
	}

	ticker := time.NewTicker(c.peerFrequency)

	defer ticker.Stop()

	for range ticker.C {
		log.Printf("check the start => %s",time.Now().String())
		group := &sync.WaitGroup{}
		c.mux.RLock()
		if len(c.malfunction) > 0 {
			//检查故障服务器状态
			for _, peer := range c.malfunction {
				addr := peer.addr
				group.Add(1)
				go func(addr net.Addr) {
					defer group.Done()
					frequency := 3
					//尝试三次
					for frequency >= 0 {
						conn, err := net.DialTimeout(addr.Network(), addr.String(), time.Second * 10);
						if err != nil {
							frequency --
							continue
						}
						conn.Close()
						break
					}
					//如果小于0 ，标识
					if frequency >= 0 {
						log.Printf("memcache server restore => %s", addr.String())
						c.mux.Lock()
						peer := c.malfunction[addr.String()]
						delete(c.malfunction, addr.String())
						c.peers[addr.String()] = peer
						c.ring.AddNode(addr.String(),peer.weight)
						c.ring.Generate()
						c.mux.Unlock()
					}
				}(addr)
			}
		}
		if len(c.peers) > 0 {
			//检查可用的服务器状态
			for _, peer := range c.peers {
				addr := peer.addr
				c.mux.RUnlock()
				group.Add(1)
				go func(addr net.Addr) {
					defer group.Done()
					frequency := 3
					//尝试三次
					for frequency >= 0 {
						conn, err := net.DialTimeout(addr.Network(), addr.String(), time.Second * 10);
						if err != nil {
							frequency --
							continue
						}
						conn.Close()
						break
					}
					//如果小于0 ，标识
					if frequency < 0 {
						log.Printf("memcache server malfunction => %s", addr.String())
						c.mux.Lock()
						peer := c.peers[addr.String()]
						delete(c.peers, addr.String())
						c.malfunction[addr.String()] = peer
						c.ring.DeleteNode(addr.String())
						c.ring.Generate()
						c.mux.Unlock()
					}
				}(addr)
				c.mux.RLock()
			}
		}
		c.mux.RUnlock()
		group.Wait()
		log.Printf("check the end => %s",time.Now().String())
	}
}
