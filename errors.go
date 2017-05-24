package memcached

import "errors"

var (
	ErrClosed      = errors.New("memcached: client is closed")
	ErrPoolTimeout = errors.New("memcached: connection pool timeout")

	ErrPoolExhausted = errors.New("memcached: connection pool exhausted")
	ErrPoolClosed    = errors.New("memcached: connection pool closed")
	ErrConnClosed    = errors.New("memcached: connection closed")

	ErrUnrecognizedCommand = errors.New("memcached: Unrecognized command")
	ErrClientError = errors.New("memcached: Client error")
	ErrServerError = errors.New("memcached: Server error")
	ErrServerExisted = errors.New("Server existed.")
	ErrNotServer	 = errors.New("Server empty.")
	ErrResultFormatError = errors.New("memcache : The results can not be resolved.")
	ErrNotStored = errors.New("memcache: NOT_STORED")
	ErrExistError = errors.New("memcache:EXIST")
	ErrKeyTooLong = errors.New("Key too long.")
	ErrCacheNotFound = errors.New("memcache:NOT_FOUND")
)
