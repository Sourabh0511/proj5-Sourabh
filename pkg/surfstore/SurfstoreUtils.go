package surfstore

import (
	"io"
	"io/fs"
	"io/ioutil"
	"log"
	"os"
	"strings"
)

type FileTagInfo struct {
	op            int // 0: create, 1: update, 2: delete, 3: no change
	blockHashList []string
	fileName      string
	version       int32
}

func checkIfFileValid(f fs.FileInfo) bool {
	if f.IsDir() {
		return false
	}
	if strings.Contains(f.Name(), "/") {
		return false
	}
	if strings.Contains(f.Name(), ",") {
		return false
	}
	return true
}

func getFileBlockHashList(client RPCClient, fileName string) []string {
	blocks := getFileBlocks(client, fileName)
	outList := make([]string, 0)
	for _, block := range blocks {
		blockHash := GetBlockHashString(block)
		outList = append(outList, blockHash)
	}

	return outList
}

func getFileBlocks(client RPCClient, fileName string) [][]byte {
	f, err := os.Open(ConcatPath(client.BaseDir, fileName))
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	buf := make([]byte, client.BlockSize)
	var mainbuf []byte

	var arr [][]byte

	for {
		bytesRead, err := f.Read(buf)
		if err != nil {
			if err != io.EOF {
				log.Fatal(err)
			}
			break
		}
		tempbuf := buf[:bytesRead]
		mainbuf = append(mainbuf, tempbuf...)
		if len(mainbuf) >= client.BlockSize {
			arr = append(arr, mainbuf[:client.BlockSize])
			mainbuf = mainbuf[client.BlockSize:]
		}
	}
	if len(mainbuf) > 0 {
		arr = append(arr, mainbuf)
	}
	return arr
}

func checkArraysEquality(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}

func checkForFileUpdate(fileName string, localFileMetaMap map[string]*FileMetaData, fileBlockHashList []string) bool {
	localBlockHashList := localFileMetaMap[fileName].BlockHashList
	return checkArraysEquality(localBlockHashList, fileBlockHashList)
}

func getNewBlocksFromServer(client RPCClient, filename string, serverFileMetaMap map[string]*FileMetaData) [][]byte {
	newBlocks := make([][]byte, 0)
	blockStoreMap := make(map[string][]string)

	serverBlockHashList := serverFileMetaMap[filename].BlockHashList
	client.GetBlockStoreMap(serverBlockHashList, &blockStoreMap)
	// log.Println("serverBlockHL", serverBlockHashList)
	// log.Println("serverStoreMap", blockStoreMap)
	var blockMap = make(map[string]Block)
	for sAddr, blockHashList := range blockStoreMap {
		for _, blockHash := range blockHashList {
			var block Block
			client.GetBlock(blockHash, sAddr[len("blockstore"):], &block)
			// newBlocks = append(newBlocks, block.BlockData)
			blockMap[blockHash] = block
		}
	}
	// log.Println("blockMap", blockMap)
	for _, blockHash := range serverBlockHashList {
		newBlocks = append(newBlocks, blockMap[blockHash].BlockData)
	}

	return newBlocks
}

func getRespServer(blockStoreMap map[string][]string, blockHash string) string { //BlockStoreMap
	for servAdd, blockHashList := range blockStoreMap {
		for _, bh := range blockHashList {
			if bh == blockHash {
				return servAdd
			}
		}
	}
	return ""
}

func processTaggedFiles(client RPCClient, taggedFiles []FileTagInfo, localFileMetaMap *map[string]*FileMetaData) {

	serverFileMetaMap := make(map[string]*FileMetaData)
	client.GetFileInfoMap(&serverFileMetaMap)
	for _, tagged_file := range taggedFiles {
		fn := tagged_file.fileName
		if tagged_file.op == 3 {
			_, ok := serverFileMetaMap[fn]
			// fmt.Println("Ok value is:", ok)
			if ok {
				if serverFileMetaMap[fn].Version == tagged_file.version {
					continue
				}
			}
		}
		blocks := make([][]byte, 0)
		hblocks := make([]string, 0)
		// var blockStoreAdd string
		// client.GetBlockStoreAddr(&blockStoreAdd)

		var blockStrMap map[string][]string
		// fmt.Println("Server file metamap")
		// fmt.Println(serverFileMetaMap)
		// fmt.Println("Trying to access serverBlockHashList")
		// fmt.Println(serverFileMetaMap[fn])
		// if _, ok := serverFileMetaMap[fn]; !ok {
		// 	fmt.Println("Does not exist")
		// }
		// serverBlockHashList := serverFileMetaMap[fn].BlockHashList
		// fmt.Println(serverBlockHashList)
		// client.GetBlockStoreMap(serverBlockHashList, &blockStrMap)

		// if tagged_file.op == 0 {
		// 	fmt.Println("Case when the file is being uploaded the first time")
		// 	blocks = getFileBlocks(client, fn)
		// 	hblocks = getFileBlockHashList(client, fn)
		// 	client.GetBlockStoreMap(hblocks, &blockStrMap)
		// 	for ind := 0; ind < len(blocks); ind++ {
		// 		var succ bool
		// 		blockStoreAdd := getRespServer(blockStrMap, hblocks[ind])
		// 		client.PutBlock(&Block{BlockData: blocks[ind], BlockSize: int32(client.BlockSize)}, blockStoreAdd[len("blockstore"):], &succ)
		// 	}

		// }
		if tagged_file.op != 2 { //&& tagged_file.op != 0
			blocks = getFileBlocks(client, fn)
			hblocks = getFileBlockHashList(client, fn)
			client.GetBlockStoreMap(hblocks, &blockStrMap)
			// fmt.Println("First time after getting file blocks", blocks)
			var blockHashesPresent []string
			for k, v := range blockStrMap {
				var bHP []string
				client.HasBlocks(v, k[len("blockstore"):], &bHP) //Need to check this HasBlocks
				blockHashesPresent = append(blockHashesPresent, bHP...)
			}

			// fmt.Println("Block hashes present are:", blockHashesPresent)
			ind1, ind2 := 0, 0
			for {
				if ind1 >= len(hblocks) || ind2 >= len(blockHashesPresent) {
					break
				}
				if hblocks[ind1] == blockHashesPresent[ind2] {
					ind1 += 1
					ind2 += 1
				} else {
					var succ bool
					blockStoreAdd := getRespServer(blockStrMap, hblocks[ind1])
					// fmt.Println("Block store address is:", blockStoreAdd)
					client.PutBlock(&Block{BlockData: blocks[ind1], BlockSize: int32(client.BlockSize)}, blockStoreAdd[len("blockstore"):], &succ)
					ind1 += 1
				}
			}
			for {
				if ind1 >= len(hblocks) {
					break
				}
				var succ bool
				blockStoreAdd := getRespServer(blockStrMap, hblocks[ind1])
				// fmt.Println("Block store address is:", blockStoreAdd)
				client.PutBlock(&Block{BlockData: blocks[ind1], BlockSize: int32(client.BlockSize)}, blockStoreAdd[len("blockstore"):], &succ)
				ind1 += 1

			}
		}
		// Updating file meta store
		fileMetaData := FileMetaData{}
		fileMetaData.Filename = fn
		fileMetaData.Version = tagged_file.version
		fileMetaData.BlockHashList = tagged_file.blockHashList
		var updatedVersion int32
		client.UpdateFile(&fileMetaData, &updatedVersion)
		// fmt.Println("Updated version:", updatedVersion)
		if updatedVersion != -1 {
			(*localFileMetaMap)[fn] = &fileMetaData

		} else {
			if tagged_file.op != 2 {

				// newBlocks := getNewBlocksFromServer(client, fn, serverFileMetaMap, blockStoreAdd, hblocks, blocks)
				newBlocks := getNewBlocksFromServer(client, fn, serverFileMetaMap)
				if checkTombStoneCase(serverFileMetaMap[fn].BlockHashList) {
					fp := ConcatPath(client.BaseDir, fn)
					_, err2 := os.Stat(fp)
					if err2 != nil {
						continue
					}
					os.Remove(fp)
				} else {
					f1, err := os.Create(ConcatPath(client.BaseDir, fn))
					log.Println("Should come here if writing new file")
					if err != nil {
						log.Println("error creating file")
					}
					defer f1.Close()
					// log.Println(blocks)
					// log.Println(newBlocks)
					for _, b := range newBlocks {
						f1.Write(b)
					}
					f1.Sync()
				}
			}
			(*localFileMetaMap)[fn] = serverFileMetaMap[fn]

		}

	}
	// fmt.Println("inside process tag files")
	// PrintMetaMap(*localFileMetaMap)
}

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
	// panic("todo")
	// fileMap := make(map[string]*FileMetaData)
	localFileMetaMap, _ := LoadMetaFromMetaFile(client.BaseDir)

	files, err := ioutil.ReadDir(client.BaseDir)
	if err != nil {
		log.Fatal(err)
	}

	var fileList []string
	var localFilExist = make(map[string]bool)
	for _, f := range files {
		if checkIfFileValid(f) {
			if f.Name() == DEFAULT_META_FILENAME {
				continue
			}
			fileList = append(fileList, f.Name())
			localFilExist[f.Name()] = true
		}
	}

	tagFiles := make([]FileTagInfo, 0)

	for _, fn := range fileList {
		_, fileExists := localFileMetaMap[fn]
		if !fileExists {
			tagFiles = append(tagFiles, FileTagInfo{
				op:            0,
				fileName:      fn,
				version:       1,
				blockHashList: getFileBlockHashList(client, fn),
			})

		} else {

			fileBlockHashList := getFileBlockHashList(client, fn)
			if !checkForFileUpdate(fn, localFileMetaMap, fileBlockHashList) {
				tagFiles = append(tagFiles, FileTagInfo{
					op:            1,
					fileName:      fn,
					version:       localFileMetaMap[fn].Version + 1,
					blockHashList: fileBlockHashList,
				})
			} else {
				tagFiles = append(tagFiles, FileTagInfo{
					op:            3,
					fileName:      fn,
					version:       localFileMetaMap[fn].Version,
					blockHashList: fileBlockHashList,
				})

			}
		}
	}

	for fName := range localFileMetaMap {
		_, fileExists := localFilExist[fName]
		if !fileExists && !checkTombStoneCase(localFileMetaMap[fName].BlockHashList) {
			tagFiles = append(tagFiles, FileTagInfo{
				op:            2,
				fileName:      fName,
				version:       localFileMetaMap[fName].Version + 1,
				blockHashList: []string{"0"},
			})
		}
	}
	// fmt.Println("Tagged files are:", tagFiles)
	processTaggedFiles(client, tagFiles, &localFileMetaMap)

	// Get files present on server but not local
	tagFileMap := make(map[string]bool)
	for _, tag_file := range tagFiles {
		tagFileMap[tag_file.fileName] = true
	}

	serverFileMetaMap := make(map[string]*FileMetaData)
	client.GetFileInfoMap(&serverFileMetaMap)
	// var blockStoreAdd string
	// client.GetBlockStoreAddr(&blockStoreAdd)
	for fName := range serverFileMetaMap {
		_, fileExists := tagFileMap[fName]
		if !fileExists {
			// Check tombstone
			serverBlockHashList := serverFileMetaMap[fName].BlockHashList
			if checkTombStoneCase(serverBlockHashList) {
				continue
			}

			newBlocks := make([]Block, 0)
			var blockStrMap map[string][]string
			var blockMap = make(map[string]Block)

			client.GetBlockStoreMap(serverBlockHashList, &blockStrMap)
			//iterate over the keys, key is server address,
			//iterate over hashlist, get the hash from the server
			// client.GetBlock(b,key,&blocl)
			//keep one more map key will be blockhashvalue and val will be actual block value
			//ordering will be correct in the hashlist
			for sAddr, blockHashList := range blockStrMap {
				for _, blockHash := range blockHashList {
					var block Block
					client.GetBlock(blockHash, sAddr[len("blockstore"):], &block)
					// newBlocks = append(newBlocks, block.BlockData)
					blockMap[blockHash] = block
				}
			}

			for _, blockHash := range serverBlockHashList {
				newBlocks = append(newBlocks, blockMap[blockHash])
			}

			// for _, b := range serverBlockHashList {
			// 	var block Block
			// 	client.GetBlock(b, blockStoreAdd, &block)
			// 	newBlocks = append(newBlocks, block.BlockData)
			// }
			f1, err := os.Create(client.BaseDir + "/" + fName)
			if err != nil {
				log.Println("error creating file")
			}
			defer f1.Close()
			for _, b := range newBlocks {
				f1.Write(b.BlockData)
			}
			f1.Sync()

			localFileMetaMap[fName] = serverFileMetaMap[fName]
		} else if _, ok := localFileMetaMap[fName]; !ok || localFileMetaMap[fName].Version < serverFileMetaMap[fName].Version {
			//pull from remote server file version should be kept same as the server  can be combined in the above condition
			// fmt.Println("Inside here when version is lesser", fName)
			// newBlocks := make([][]byte, 0)
			serverBlockHashList := serverFileMetaMap[fName].BlockHashList
			if checkTombStoneCase(serverBlockHashList) {
				continue
			}
			newBlocks := make([]Block, 0)
			var blockStrMap map[string][]string
			var blockMap = make(map[string]Block)
			client.GetBlockStoreMap(serverBlockHashList, &blockStrMap)
			// for _, b := range serverBlockHashList {
			// 	var block Block
			// 	client.GetBlock(b, blockStoreAdd, &block)
			// 	newBlocks = append(newBlocks, block.BlockData)
			// }
			for sAddr, blockHashList := range blockStrMap {
				for _, blockHash := range blockHashList {
					var block Block
					client.GetBlock(blockHash, sAddr[len("blockstore"):], &block)
					// newBlocks = append(newBlocks, block.BlockData)
					blockMap[blockHash] = block
				}
			}

			for _, blockHash := range serverBlockHashList {
				newBlocks = append(newBlocks, blockMap[blockHash])
			}

			f1, err := os.Create(client.BaseDir + "/" + fName)
			if err != nil {
				log.Println("error creating file")
			}
			defer f1.Close()
			for _, b := range newBlocks {
				f1.Write(b.BlockData)
			}
			f1.Sync()

			localFileMetaMap[fName] = serverFileMetaMap[fName]
			// PrintMetaMap(localFileMetaMap)

		}
	}
	// processTaggedFiles(client, tagFiles, &localFileMetaMap)
	WriteMetaFile(localFileMetaMap, client.BaseDir)

}

func checkTombStoneCase(blockHashList []string) bool {
	if len(blockHashList) > 0 && blockHashList[0] == "0" {
		return true
	}
	return false
}
