package surfstore

import (
	context "context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	sync "sync"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type BlockStore struct {
	BlockMap map[string]*Block
	UnimplementedBlockStoreServer
	mlock sync.Mutex
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	// panic("todo")
	bs.mlock.Lock()
	if _, ok := (bs.BlockMap)[blockHash.Hash]; ok {
		// fmt.Println("Block val inside GetBlock BlockStore", blockHash.Hash)
		bs.mlock.Unlock()
		return bs.BlockMap[blockHash.Hash], nil
	}
	bs.mlock.Unlock()
	return nil, fmt.Errorf("block not found")
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	// panic("todo")
	// fmt.Println("In put block")
	hashBytes := sha256.Sum256(block.BlockData)
	hash := hex.EncodeToString(hashBytes[:])
	bs.mlock.Lock()
	bs.BlockMap[hash] = block
	bs.mlock.Unlock()
	return &Success{
		Flag: true,
	}, nil
}

// Given a list of hashes “in”, returns a list containing the
// subset of in that are stored in the key-value store
func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	// panic("todo")
	var hashList []string
	bs.mlock.Lock()
	for _, hash := range blockHashesIn.Hashes {
		if _, ok := (bs.BlockMap)[hash]; ok {
			hashList = append(hashList, hash)
		}
	}
	bs.mlock.Unlock()
	return &BlockHashes{
		Hashes: hashList,
	}, nil
}

func (bs *BlockStore) GetBlockHashes(ctx context.Context, _ *emptypb.Empty) (*BlockHashes, error) {
	// panic("todo")
	// hashes := make([]string, 0)  check for error thingy as well here
	bHashes := &BlockHashes{}
	bHashes.Hashes = make([]string, 0)
	for k := range bs.BlockMap {
		bHashes.Hashes = append(bHashes.Hashes, k)
	}
	return bHashes, nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
