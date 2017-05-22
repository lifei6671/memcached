package memcached

import (
	"crypto/sha1"
	"strconv"
	"sort"
)


type PeerList []*MemcachedPeer

func (p PeerList) Len() int           { return len(p) }
func (p PeerList) Less(i, j int) bool { return p[i].hash < p[j].hash }
func (p PeerList) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p PeerList) Sort()              { sort.Sort(p) }

type peerRing struct {
	defaultSpots int
	length       int
	peers PeerList
}
func NewRing(n int) (h *peerRing) {
	h = new(peerRing)
	h.defaultSpots = n
	return
}


func (p *peerRing) AddNode(peer *MemcachedPeer,s int) error {
	tSpots := p.defaultSpots * s
	hash := sha1.New()
	for i := 1; i <= tSpots; i++ {
		hash.Write([]byte(peer.addr.String() + ":" + strconv.Itoa(i)))
		hashBytes := hash.Sum(nil)

		peer.hash = uint(hashBytes[19]) | uint(hashBytes[18])<<8 | uint(hashBytes[17])<<16 | uint(hashBytes[16])<<24

		p.peers = append(p.peers, peer)

		hash.Reset()
	}
	return nil
}

func (h *peerRing) Bake() {
	h.peers.Sort()
	h.length = len(h.peers)
}

func (h *peerRing) Hash(s string) *MemcachedPeer {
	if len(h.peers) <= 0 {
		return nil
	}
	hash := sha1.New()
	hash.Write([]byte(s))
	hashBytes := hash.Sum(nil)
	v := uint(hashBytes[19]) | uint(hashBytes[18])<<8 | uint(hashBytes[17])<<16 | uint(hashBytes[16])<<24
	i := sort.Search(h.length, func(i int) bool { return h.peers[i].hash >= v })

	if i == h.length {
		i = 0
	}

	return h.peers[i]
}
