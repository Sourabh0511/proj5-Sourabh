package surfstore

import (
	context "context"
	sync "sync"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap        map[string]*FileMetaData
	BlockStoreAddrs    []string
	ConsistentHashRing *ConsistentHashRing
	mlock              sync.Mutex
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	// panic("todo")
	return &FileInfoMap{
		FileInfoMap: m.FileMetaMap,
	}, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	// panic("todo")

	m.mlock.Lock()
	if _, ok := (m.FileMetaMap)[fileMetaData.Filename]; ok {
		// fmt.Println("Meta store file version:", m.FileMetaMap[fileMetaData.Filename].Version)
		// fmt.Println("Local file version:", fileMetaData.Version)
		if (m.FileMetaMap[fileMetaData.Filename].Version + 1) != fileMetaData.Version {
			m.mlock.Unlock()
			// fmt.Println("Going inside")
			return &Version{
				Version: -1,
			}, nil
		}
	}

	m.FileMetaMap[fileMetaData.Filename] = fileMetaData
	m.mlock.Unlock()
	return &Version{
		Version: fileMetaData.Version,
	}, nil
}

// func (m *MetaStore) GetBlockStoreAddr(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddr, error) {
// 	// panic("todo")
// 	return &BlockStoreAddr{Addr: m.BlockStoreAddr}, nil
// }

func (m *MetaStore) GetBlockStoreMap(ctx context.Context, blockHashesIn *BlockHashes) (*BlockStoreMap, error) {
	// panic("todo")
	// you need to call getResponsible server from here
	// make(map[string]*BlockHashes)
	// fmt.Println("Input hashes for MS GMSMap:", blockHashesIn)
	blockStoreMap := &BlockStoreMap{}
	blockStoreMap.BlockStoreMap = make(map[string]*BlockHashes)
	for _, hash := range blockHashesIn.GetHashes() { //.hashes
		resp_ser := m.ConsistentHashRing.GetResponsibleServer(hash)
		if _, ok := blockStoreMap.BlockStoreMap[resp_ser]; !ok {
			blockStoreMap.BlockStoreMap[resp_ser] = &BlockHashes{}
			blockStoreMap.BlockStoreMap[resp_ser].Hashes = []string{}
		}
		blockStoreMap.BlockStoreMap[resp_ser].Hashes = append(blockStoreMap.BlockStoreMap[resp_ser].Hashes, hash)
	}
	// fmt.Println("Get block store map in meta store:", blockStoreMap)
	return blockStoreMap, nil
}

func (m *MetaStore) GetBlockStoreAddrs(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddrs, error) {
	// panic("todo")
	return &BlockStoreAddrs{BlockStoreAddrs: m.BlockStoreAddrs}, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddrs []string) *MetaStore {
	return &MetaStore{
		FileMetaMap:        map[string]*FileMetaData{},
		BlockStoreAddrs:    blockStoreAddrs,
		ConsistentHashRing: NewConsistentHashRing(blockStoreAddrs),
	}
}
