package memcached

import (
	"sync"
	"errors"
	"time"
	"fmt"
	"log"
	"bytes"
)

const MAX_POOL_SIZE  = 200
const MIN_POOL_SIZE  = 5
//默认的虚拟节点数量
const VIRTUAL_SIZE = 100

var (
	ErrServerExisted = errors.New("Server existed.")
	ErrNotServer	 = errors.New("Server empty.")
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

type MemcachedClient struct {
	mux sync.RWMutex
	peers *peerRing
	readTimeout  time.Duration
	writeTimeout time.Duration
	timeout time.Duration
	maxConnections int
	minConnections int
}

type perrs  []*MemcachedPeer

func NewMemcachedClient(servers ...string) *MemcachedClient {
	addrs := make([]*MemcachedPeer,len(servers))

	for index,server := range servers {
		addr,err := ResolveMemcachedAddr(server)

		if err != nil {
			panic(err.Error())
		}
		peer :=  NewMemcachedPeer(addr)
		peer.max = MAX_POOL_SIZE
		peer.min = MIN_POOL_SIZE

		addrs[index] = peer
	}
	ring := NewRing(VIRTUAL_SIZE)
	ring.peers = addrs

	return &MemcachedClient{  mux : sync.RWMutex{},peers: ring ,maxConnections: MAX_POOL_SIZE,minConnections:MIN_POOL_SIZE, readTimeout : time.Second * 30, writeTimeout: time.Second * 30, timeout: time.Second * 30}
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

	if err := c.peers.AddNode(peer,weight);err != nil{
		return err
	}

	return nil
}

func (c *MemcachedClient) SetReadTimeout(readTimeout time.Duration) {
	c.readTimeout = readTimeout
}

func (c *MemcachedClient) SetWriteTimeout(writeTimeout time.Duration)  {
	c.writeTimeout = writeTimeout
}

func (c *MemcachedClient) SetMaxConnection(n int)  {
	c.maxConnections = n
}

func (c *MemcachedClient) SetMinConnection(n int)  {
	c.minConnections = n
}

func (c *MemcachedClient) SetTimeout(timeout time.Duration)  {
	c.timeout = timeout
}

func (c *MemcachedClient) InitMemcachedClient() {
	if c.maxConnections <= 0 {
		c.maxConnections = MAX_POOL_SIZE
	}
	if c.minConnections <= 0 {
		c.minConnections = MIN_POOL_SIZE
	}

	if len(c.peers.peers) > 0{
		for _,peer := range c.peers.peers {
			peer.InitPeer(c.maxConnections,c.minConnections,c.timeout)
		}
	}
}

func (c *MemcachedClient) pickServer(key string) *MemcachedPeer {
	return c.peers.Hash(key)
}

func (c *MemcachedClient) Set(key string,b []byte,flag bool,expire time.Duration) error {

	peer := c.peers.Hash(key)
	if peer != nil {
		conn := peer.getFreeConn(c.timeout)
		defer peer.Freed(conn)

		conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
		n,err := fmt.Fprintf(conn,"%s %s %d %d %d\r\n", "set", key, 0, 0, len(b))

		if err != nil {
			log.Println("Set => ",n,err)
			return err
		}
		conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
		n,err = conn.Write(b)

		if err != nil {
			log.Println("Set => ",n,err)
			return err
		}

		conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
		n,err = conn.Write(crlf)

		if err != nil {
			log.Println("Set => ",n,err)
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
		}
	}

	return ErrNotServer
}

func (c *MemcachedClient) Get(key string) ([]byte,error) {
	peer := c.peers.Hash(key)
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
				return b,err
			}
			buf.Write(b[:n])

			if bytes.HasSuffix(buf.Bytes(),resultEnd) {
				_,err := buf.ReadBytes('\n')
				if err != nil {
					return nil,err
				}
				body := buf.Bytes()
				return body,nil
			}
		}

	}
	return nil,ErrNotServer
}





