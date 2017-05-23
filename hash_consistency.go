package memcached

import (
	"sort"
	"math"
	"crypto/sha1"
	"strconv"
)

const DefaultReplicas = 500

type node struct {
	hash uint
	addr string
}

type nodeList []node

func (p nodeList) Len() int           { return len(p) }
func (p nodeList) Less(i, j int) bool { return p[i].hash < p[j].hash }
func (p nodeList) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p nodeList) Sort()              { sort.Sort(p) }


type HashRing struct {
	replicas int
	weights map[string]int
	nodes nodeList
}

func NewHashRing(replicas int) *HashRing {
	if replicas <= 0 {
		replicas = DefaultReplicas
	}
	h := &HashRing{
		replicas : replicas,
		weights:     make(map[string]int),
	}
	return h
}

func (h *HashRing) AddNode(key string,weight int) *HashRing {
	h.weights[key] = weight
	return h
}

func (h *HashRing) DeleteNode(key string) *HashRing {
	delete(h.weights,key)
	return h
}

func (h *HashRing) GetNode(key string) string {
	if len(h.nodes) == 0 {
		return ""
	}

	hash := sha1.New()
	hash.Write([]byte(key))
	hashBytes := hash.Sum(nil)
	v := cypherHash(hashBytes)
	i := sort.Search(len(h.nodes), func(i int) bool { return h.nodes[i].hash >= v })

	if i == len(h.nodes) {
		i = 0
	}

	return h.nodes[i].addr
}

func (h *HashRing) Generate() {
	totalWeight := 0

	//计算总的需要创建的节点
	for _,w := range h.weights {
		totalWeight += w
	}

	totalVirtualSpots := h.replicas * len(h.weights)

	h.nodes = make([]node,len(h.weights))

	for k,w := range h.weights {
		spots := int(math.Floor(float64(w) / float64(totalWeight) * float64(totalVirtualSpots)))

		for i := 1; i <= spots; i++ {
			hash := sha1.New()
			hash.Write([]byte(k + ":" + strconv.Itoa(i)))
			hashBytes := hash.Sum(nil)
			n := node{
				addr:   k,
				hash: cypherHash(hashBytes),
			}
			h.nodes = append(h.nodes, n)
			hash.Reset()
		}

	}
	h.nodes.Sort()
}

func cypherHash(b []byte) uint {
	if len(b) < 4 {
		return 0
	}
	v := uint(b[19]) | uint(b[18])<<8 | uint(b[17])<<16 | uint(b[16])<<24
	return v
}




















