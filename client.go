package memcached

import (
	"sync"
	"errors"
	"time"
	"fmt"
	"log"
	"bytes"
	"strings"
	"strconv"
)
//https://github.com/go-redis/redis/blob/master/internal/pool/pool.go

const MAX_POOL_SIZE  = 200
const MIN_POOL_SIZE  = 5
//默认的虚拟节点数量
const VIRTUAL_SIZE = 500

var (
	ErrServerExisted = errors.New("Server existed.")
	ErrNotServer	 = errors.New("Server empty.")
	ErrResultFormatError = errors.New("Memcache Response Error : The results can not be resolved.")
	ErrNotStored = errors.New("Memcache Response Error: NOT_STORED")
	ErrKeyTooLong = errors.New("Key too long.")
	ErrCacheNotFound = errors.New("Memcache Response Error:NOT_FOUND")
)
var (
	crlf            = []byte("\r\n")
	space           = []byte(" ")
	resultOK        = []byte("OK\r\n")
	resultStored    = []byte("STORED\r\n")
	resultNotStored = []byte("NOT_STORED\r\n")
	resultExists    = []byte("EXISTS\r\n")
	resultNotFound  = []byte("NOT_FOUND\r\n")
	resultDeleted   = []byte("DELETED\r\n")
	resultEnd       = []byte("END\r\n")
	resultOk        = []byte("OK\r\n")
	resultTouched   = []byte("TOUCHED\r\n")

	resultClientErrorPrefix = []byte("CLIENT_ERROR ")
)
//MemcachedClient struct.
type MemcachedClient struct {
	mux sync.RWMutex
	//服务选择算法.
	ring *HashRing
	//服务映射.
	peers map[string]*MemcachedPeer
	//读超时时间.
	readTimeout  time.Duration
	//写超时时间.
	writeTimeout time.Duration
	//连接超时时间.
	timeout time.Duration
	//最大连接数.
	maxConnections int
	//最小保持的连接数.
	minConnections int
	enableCompression bool
	//心跳检查时间间隔.
	heartbeatFrequency time.Duration
}

func NewMemcachedClient(servers ...string) *MemcachedClient {
	addrs := make(map[string]*MemcachedPeer,len(servers))
	ring := NewHashRing(VIRTUAL_SIZE)

	for _,server := range servers {
		addr,err := ResolveMemcachedAddr(server)

		if err != nil {
			panic(err.Error())
		}
		peer :=  NewMemcachedPeer(addr)
		peer.max = MAX_POOL_SIZE
		peer.min = MIN_POOL_SIZE

		addrs[addr.String()] = peer
		ring.AddNode(addr.String(),1)
	}
	ring.Generate()

	return &MemcachedClient{
		mux : sync.RWMutex{},
		peers: make(map[string]*MemcachedPeer,10) ,
		ring: ring,
		maxConnections: MAX_POOL_SIZE,
		minConnections: MIN_POOL_SIZE,
		readTimeout : time.Second * 30,
		writeTimeout: time.Second * 30,
		timeout: time.Second * 30,
		enableCompression: false,
	}
}

//新增一个 Memcached 服务器.
func (c *MemcachedClient) AddServer(server string,weight int) error {
	addr,err := ResolveMemcachedAddr(server)

	if err != nil {
		return err
	}

	c.mux.Lock()
	defer c.mux.Unlock()

	peer := NewMemcachedPeer(addr)
	peer.max = MAX_POOL_SIZE
	peer.min = MIN_POOL_SIZE

	c.peers[peer.addr.String()] = peer
	c.ring.AddNode(peer.addr.String(),weight).Generate()

	return nil
}

//设置读超时时间.
func (c *MemcachedClient) SetReadTimeout(readTimeout time.Duration) {
	c.readTimeout = readTimeout
}

//设置写超时时间.
func (c *MemcachedClient) SetWriteTimeout(writeTimeout time.Duration)  {
	c.writeTimeout = writeTimeout
}

//设置最大连接数.
func (c *MemcachedClient) SetMaxConnection(n int)  {
	c.maxConnections = n
}

//设置最小生存连接数.
func (c *MemcachedClient) SetMinConnection(n int)  {
	c.minConnections = n
}

//设置连接超时.
func (c *MemcachedClient) SetTimeout(timeout time.Duration)  {
	c.timeout = timeout
}

//是否启用压缩.
//func (c *MemcachedClient) EnableCompression (ec bool)  {
//	c.enableCompression = ec
//}

//超时重试次数.
func (c *MemcachedClient) SetRetryInterval(interval int) *MemcachedClient {
	c.mux.Lock()
	defer c.mux.Unlock()
	for _,peer := range c.peers {
		peer.retryInterval = interval
	}

	return c
}

//初始化连接池.
func (c *MemcachedClient) InitMemcachedClient() {
	if c.maxConnections <= 0 {
		c.maxConnections = MAX_POOL_SIZE
	}
	if c.minConnections <= 0 {
		c.minConnections = MIN_POOL_SIZE
	}

	if len(c.peers) > 0{
		c.mux.Lock()
		defer c.mux.Unlock()

		for _,peer := range c.peers {
			peer.InitPeer(c.maxConnections,c.minConnections,c.timeout)
		}
	}
}

//关闭所有服务的连接，并清空连接池.
func (c *MemcachedClient) Close()  {
	c.mux.Lock()
	defer c.mux.Unlock()

	end := &sync.WaitGroup{}

	for _,peer := range c.peers {
		end.Add(1)
		go func(peer *MemcachedPeer,end *sync.WaitGroup) {
			defer 	end.Done()
			for{
				select {
				case conn := <- peer.pool :
					{
						err := conn.Close()
						if err != nil {
							log.Println("Close failure :",conn.RemoteAddr().String(),err)
						}
						log.Println("Closed:",conn.RemoteAddr().String())
						break
					}
				case t := <- time.After(time.Second * 1):
					log.Println(t.String())
					return
				}
			}
		}(peer,end)
	}
	end.Wait()
}

func (c *MemcachedClient) pickServer(key string) *MemcachedPeer {
	c.mux.RLock()
	defer c.mux.RUnlock()

	return c.peers[c.ring.GetNode(key)]
}

//设置.
func (c *MemcachedClient) Set(key string,v []byte,flag int32,expire time.Duration) error {

	if !checkKey(key) {
		return ErrKeyTooLong
	}
	entry := &Entry{
		Key:key,
		Flags: uint32(flag),
		Expiration: int32(expire.Seconds()),
		Value: v,
	}

	return c.populateAction(entry,"set")
}

//增加.
func (c *MemcachedClient) Add(key string,v []byte,flag int32,expire time.Duration) error  {
	if !checkKey(key) {
		return ErrKeyTooLong
	}
	entry := &Entry{
		Key:key,
		Flags: uint32(flag),
		Expiration: int32(expire.Seconds()),
		Value: v,
	}

	return c.populateAction(entry,"add")
}

//替换.
func (c *MemcachedClient) Replace(key string,v []byte,flag int32,expire time.Duration) error {
	if !checkKey(key) {
		return ErrKeyTooLong
	}
	entry := &Entry{
		Key:key,
		Flags: uint32(flag),
		Expiration: int32(expire.Seconds()),
		Value: v,
	}

	return c.populateAction(entry,"replace")
}

//获取指定键的值.
func (c *MemcachedClient) Get(key string) (*Entry,error) {
	peer := c.pickServer(key)
	if peer != nil {

		conn := peer.getFreeConn(c.timeout)
		defer peer.Freed(conn)

		_, err := fmt.Fprintf(conn, "%s %s\r\n", "get", key)
		if err != nil {
			log.Println(err)
			return nil,err
		}
		buf := bytes.NewBufferString("")

		for{
			b := make([]byte,2048)

			n,err := conn.Read(b)
			if err != nil {
				return nil,err
			}
			buf.Write(b[:n])

			if bytes.HasSuffix(buf.Bytes(),resultEnd) {
				line,err := buf.ReadString('\n')
				if err != nil {
					return nil,err
				}

				value := strings.Split(line," ")
				if len(value) < 4 {
					return nil,ErrResultFormatError
				}
				entry := &Entry{}

				entry.Key = value[1]

				flag,err := strconv.Atoi(value[2]);
				if err != nil {
					return nil,err
				}
				entry.Flags = uint32(flag)

				if strings.HasSuffix(value[3],"\r\n") {
					value[3] = strings.TrimSuffix(value[3],"\r\n")
				}
				l,err :=  strconv.Atoi(value[3])
				if err != nil {
					log.Printf("Value length:%d; Error : %s",l,err)
					return nil,ErrResultFormatError
				}

				body := make([]byte,l + 2)

				n,err = buf.Read(body)
				if err != nil {
					return nil,err
				}
				if n != l + 2 || !bytes.Equal(body[l:], crlf){
					return nil,ErrResultFormatError
				}
				entry.Value = body[:l]
				return entry,nil
			}
		}

	}
	return nil,ErrNotServer
}

//删除缓存.
func (c *MemcachedClient) Delete(key string) error {
	peer := c.pickServer(key)
	if peer != nil {
		conn := peer.getFreeConn(c.timeout)
		defer peer.Freed(conn)

		conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
		n,err := fmt.Fprintf(conn,"%s %s %d\r\n", "delete", key, 0)

		if err != nil {
			log.Printf("delete => %d %s",n,err.Error())
			return err
		}

		buf := bytes.NewBufferString("")

		for{
			b := make([]byte,2048)
			conn.SetReadDeadline(time.Now().Add(c.readTimeout))
			n,err := conn.Read(b)

			if err != nil {
				log.Println(err.Error())
				return err
			}
			buf.Write(b[:n])

			if bytes.Equal(buf.Bytes(),resultDeleted ) {
				return nil
			}
			if bytes.Equal(buf.Bytes(),resultNotFound) {
				return ErrCacheNotFound
			}
		}
	}

	return ErrNotServer
}

func (c *MemcachedClient) Increment(key string, delta uint64) (uint64,error)  {
	peer := c.pickServer(key)

	if peer != nil {
		conn := peer.getFreeConn(c.timeout)
		defer peer.Freed(conn)

		conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
		n,err := fmt.Fprintf(conn,"%s %s %d\r\n", "incr", key, delta)

		if err != nil {
			log.Printf("incr => %d %s",n,err.Error())
			return 0,err
		}

		buf := bytes.NewBufferString("")

		for{
			b := make([]byte,2048)
			conn.SetReadDeadline(time.Now().Add(c.readTimeout))
			n,err := conn.Read(b)

			if err != nil {
				log.Println(err.Error())
				return 0,err
			}
			buf.Write(b[:n])

			if bytes.Equal(buf.Bytes(),resultNotFound) {

				return 0,ErrCacheNotFound
			}
			if bytes.HasSuffix(buf.Bytes(),crlf) {
				v := buf.Bytes()[:n - 2]
				value,err := strconv.Atoi(string(v))
				return uint64(value),err
			}
		}
	}


	return 0,ErrNotServer
}

func (c *MemcachedClient) Decrement(key string, delta uint64) (uint64,error) {
	peer := c.pickServer(key)

	if peer != nil {
		conn := peer.getFreeConn(c.timeout)
		defer peer.Freed(conn)

		conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
		n,err := fmt.Fprintf(conn,"%s %s %d\r\n", "decr", key, delta)

		if err != nil {
			log.Printf("decr => %d %s",n,err.Error())
			return 0,err
		}

		buf := bytes.NewBufferString("")

		for{
			b := make([]byte,2048)
			conn.SetReadDeadline(time.Now().Add(c.readTimeout))
			n,err := conn.Read(b)

			if err != nil {
				log.Println(err.Error())
				return 0,err
			}
			buf.Write(b[:n])

			if bytes.Equal(buf.Bytes(),resultNotFound) {

				return 0,ErrCacheNotFound
			}
			if bytes.HasSuffix(buf.Bytes(),crlf) {
				v := buf.Bytes()[:n - 2]
				value,err := strconv.Atoi(string(v))
				return uint64(value),err
			}
		}
	}


	return 0,ErrNotServer
}

type MemcachedStateItem struct {
	Name string
	Value string
}

//获取指定 Memcached 的状态.
func (c *MemcachedClient) Stats(addr string,args ...string) ([]*MemcachedStateItem,error) {

	c.mux.RLock()
	defer c.mux.RUnlock()

	peer := c.peers[addr]

	if peer != nil {
		conn := peer.getFreeConn(c.timeout)
		defer peer.Freed(conn)

		conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
		if len(args) > 0 {
			n,err := fmt.Fprintf(conn,"%s %s\r\n", "stats", args[0])

			if err != nil {
				log.Printf("stats => %d %s",n,err.Error())
				return nil,err
			}
		}else{
			n,err := fmt.Fprintf(conn,"%s\r\n", "stats")

			if err != nil {
				log.Printf("stats => %d %s",n,err.Error())
				return nil,err
			}
		}


		buf := bytes.NewBufferString("")

		for{
			b := make([]byte,2048)
			conn.SetReadDeadline(time.Now().Add(c.readTimeout))
			n,err := conn.Read(b)

			if err != nil {
				log.Println(err.Error())
				return nil,err
			}
			buf.Write(b[:n])

			if bytes.HasSuffix(buf.Bytes(),resultEnd) {
				items := make([]*MemcachedStateItem,0)

				for{
					line ,err := buf.ReadBytes('\n')
					if err != nil {
						return nil,err
					}
					if bytes.HasPrefix(line,resultEnd) {
						break
					}
					if bytes.HasPrefix(line,[]byte("STAT")) {
						item := &MemcachedStateItem{}
						splits := bytes.Split(line,space)
						if len(splits) >= 3 {
							item.Name = string(splits[1])
							item.Value = string(splits[2])
							items = append(items,item)
						}
					}
				}
				return items,nil
			}
		}
	}


	return nil,ErrNotServer
}

func (c *MemcachedClient) populateAction(entry *Entry,action string) error {

	peer := c.pickServer(entry.Key)
	if peer != nil {
		conn := peer.getFreeConn(c.timeout)
		defer peer.Freed(conn)

		conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
		n,err := fmt.Fprintf(conn,"%s %s %d %d %d\r\n", action, entry.Key, entry.Flags, entry.Expiration, len(entry.Value))

		if err != nil {
			log.Printf("%s => %d %s",entry.Key,n,err.Error())
			return err
		}
		conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
		n,err = conn.Write(entry.Value)

		if err != nil {
			log.Printf("%s => %d %s",entry.Key,n,err.Error())
			return err
		}

		conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
		n,err = conn.Write(crlf)

		if err != nil {
			log.Printf("%s => %d %s",entry.Key,n,err.Error())
			return err
		}
		buf := bytes.NewBufferString("")

		for{
			b := make([]byte,2048)
			conn.SetReadDeadline(time.Now().Add(c.readTimeout))
			n,err := conn.Read(b)

			if err != nil {
				log.Println(err.Error())
				return err
			}
			buf.Write(b[:n])

			if bytes.Equal(buf.Bytes(),resultStored ) {
				return nil
			}
			if bytes.Equal(buf.Bytes(),resultNotStored) {
				return ErrNotStored
			}
			log.Println(buf.Bytes())
		}
	}

	return ErrNotServer
}

