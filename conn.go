package memcached

import (
	"net"
	"bufio"
	"time"
)

type Conn struct {
	conn net.Conn
	rw *bufio.ReadWriter
}

func NewConn(conn net.Conn) *Conn  {
	return &Conn{
		conn	: conn,
		rw 	: bufio.NewReadWriter(bufio.NewReader(conn),bufio.NewWriter(conn)),
	}
}

// Read 从连接中读取数据.
func (conn *Conn)Read(b []byte) (n int, err error)  {
	n,err = conn.conn.Read(b)
	return
}

// Write 将输入写入连接.
func (conn *Conn) Write(b []byte) (n int, err error)  {
	n,err = conn.conn.Write(b)
	return
}

// Close 关闭连接.
func (conn *Conn) Close() error  {
	return conn.conn.Close()
}
// LocalAddr 获取本地地址.
func (conn *Conn) LocalAddr() net.Addr {
	return conn.conn.LocalAddr()
}
// RemoteAddr 获取远程地址.
func (conn *Conn) RemoteAddr() net.Addr {
	return conn.conn.RemoteAddr()
}

// SetDeadline 设置超时时间.
func (conn *Conn) SetDeadline(t time.Time) error {
	return conn.conn.SetDeadline(t)
}

// SetReadDeadline 设置读超时时间.
func (conn *Conn) SetReadDeadline(t time.Time) error {
	return conn.conn.SetReadDeadline(t)
}

// SetWriteDeadline 设置写超时时间.
func (conn *Conn) SetWriteDeadline(t time.Time) error {
	return conn.conn.SetWriteDeadline(t)
}