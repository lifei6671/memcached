package memcached

import (
	"net"
	"strings"
)

func checkKey(key string) bool {
	if len(key) > 250 {
		return false
	}
	for _,v := range key {
		if v == ' ' || v == 0x7f{
			return false
		}
	}
	return true
}

//解析地址.
func ResolveMemcachedAddr(addr string) (net.Addr, error) {

	maddr := &MemcachedAddr{}

	if strings.Contains(addr,"/") {
		addr, err := net.ResolveUnixAddr("unix", addr)
		if err != nil {
			return maddr,err
		}
		maddr.str = addr.String()
		maddr.ntw = addr.Network()

		return maddr,nil
	}else{
		addr ,err := net.ResolveTCPAddr("tcp",addr)
		if err != nil {
			return maddr,err
		}
		maddr.str = addr.String()
		maddr.ntw = addr.Network()

		return maddr,nil
	}

}
