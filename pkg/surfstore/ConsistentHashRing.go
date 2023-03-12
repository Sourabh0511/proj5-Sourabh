package surfstore

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
)

type ConsistentHashRing struct {
	ServerMap map[string]string
}

func (c ConsistentHashRing) GetResponsibleServer(blockId string) string {
	// panic("todo")
	hashes := []string{}
	for h := range c.ServerMap {
		hashes = append(hashes, h)
	}
	sort.Strings(hashes)

	// fmt.Println("Len of hashes", len(hashes))
	// fmt.Println("Len of Smap", len(c.ServerMap))
	responsibleServer := ""
	for i := 0; i < len(hashes); i++ {
		// fmt.Println("h ind", i)
		if hashes[i] > blockId {
			responsibleServer = c.ServerMap[hashes[i]]
			break
		}
	}
	if responsibleServer == "" {
		responsibleServer = c.ServerMap[hashes[0]]
	}

	return responsibleServer
}

func (c ConsistentHashRing) Hash(addr string) string {
	h := sha256.New()
	h.Write([]byte(addr))
	return hex.EncodeToString(h.Sum(nil))

}

func NewConsistentHashRing(serverAddrs []string) *ConsistentHashRing {
	// panic("todo")
	consistentHashRing := ConsistentHashRing{ServerMap: make(map[string]string)}
	for _, serverName := range serverAddrs {
		serverHash := consistentHashRing.Hash("blockstore" + serverName)
		consistentHashRing.ServerMap[serverHash] = "blockstore" + serverName
	}
	return &consistentHashRing
}
