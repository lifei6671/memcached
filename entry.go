package memcached

type Entry struct {
	//键值
	Key string
	//内容
	Value []byte
	//标识
	Flags uint16
	//有效期
	Expiration int32
	CasId      uint64
}

func NewEntry(key string, b []byte, flag uint16, expire int32, casid uint64) *Entry {
	return &Entry{
		Key:        key,
		Value:      b,
		Flags:      flag,
		Expiration: expire,
		CasId:      casid,
	}
}

