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

func (conn *Conn)Read(b []byte) (n int, err error)  {
	n,err = conn.conn.Read(b)
	return
}

func (conn *Conn) Write(b []byte) (n int, err error)  {
	n,err = conn.conn.Write(b)
	return
}

func (conn *Conn) Close() error  {
	return conn.conn.Close()
}
func (conn *Conn) LocalAddr() net.Addr {
	return conn.conn.LocalAddr()
}

func (conn *Conn) RemoteAddr() net.Addr {
	return conn.conn.RemoteAddr()
}

func (conn *Conn) SetDeadline(t time.Time) error {
	return conn.conn.SetDeadline(t)
}

func (conn *Conn) SetReadDeadline(t time.Time) error {
	return conn.conn.SetReadDeadline(t)
}

func (conn *Conn) SetWriteDeadline(t time.Time) error {
	return conn.conn.SetWriteDeadline(t)
}