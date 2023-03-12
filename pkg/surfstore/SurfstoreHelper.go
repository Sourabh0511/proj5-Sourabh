package surfstore

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"path/filepath"

	_ "github.com/mattn/go-sqlite3"
)

/* Hash Related */
func GetBlockHashBytes(blockData []byte) []byte {
	h := sha256.New()
	h.Write(blockData)
	return h.Sum(nil)
}

func GetBlockHashString(blockData []byte) string {
	blockHash := GetBlockHashBytes(blockData)
	return hex.EncodeToString(blockHash)
}

/* File Path Related */
func ConcatPath(baseDir, fileDir string) string {
	return baseDir + "/" + fileDir
}

/*
	Writing Local Metadata File Related
*/

const createTable string = `create table if not exists indexes (
		fileName TEXT, 
		version INT,
		hashIndex INT,
		hashValue TEXT
	);`

const insertTuple string = `insert into indexes (fileName, version, hashIndex,hashValue) VALUES (?,?,?,?);`

// WriteMetaFile writes the file meta map back to local metadata file index.db
func WriteMetaFile(fileMetas map[string]*FileMetaData, baseDir string) error {
	// remove index.db file if it exists
	fmt.Println("Writing to DB...")
	// PrintMetaMap(fileMetas)
	outputMetaPath := ConcatPath(baseDir, DEFAULT_META_FILENAME)
	if _, err := os.Stat(outputMetaPath); err == nil {
		e := os.Remove(outputMetaPath)
		if e != nil {
			log.Fatal("Error During Meta Write Back")
		}
	}
	db, err := sql.Open("sqlite3", outputMetaPath)
	if err != nil {
		log.Fatal("Error During Meta Write Back")
	}
	statement, err := db.Prepare(createTable)
	if err != nil {
		log.Fatal("Error During Meta Write Back")
	}
	statement.Exec()
	// panic("todo")
	statement, _ = db.Prepare(insertTuple)
	for _, fileMeta := range fileMetas {
		if fileMeta.Filename != "index.db" {
			fname := fileMeta.Filename
			fvers := fileMeta.Version
			hlist := fileMeta.BlockHashList

			for i, _ := range hlist {
				bl := hlist[i]
				statement.Exec(fname, fvers, i, bl)
			}
		}
	}
	return nil
}

/*
Reading Local Metadata File Related
*/
const getDistinctFileName string = `select distinct fileName from indexes`

const getTuplesByFileName string = `select fileName, version, hashIndex, hashValue 
from indexes where fileName = ? order by hashIndex;`

// LoadMetaFromMetaFile loads the local metadata file into a file meta map.
// The key is the file's name and the value is the file's metadata.
// You can use this function to load the index.db file in this project.
func LoadMetaFromMetaFile(baseDir string) (fileMetaMap map[string]*FileMetaData, e error) {
	metaFilePath, _ := filepath.Abs(ConcatPath(baseDir, DEFAULT_META_FILENAME))
	fileMetaMap = make(map[string]*FileMetaData)
	metaFileStats, e := os.Stat(metaFilePath)
	if e != nil || metaFileStats.IsDir() {
		return fileMetaMap, nil
	}
	db, err := sql.Open("sqlite3", metaFilePath)
	if err != nil {
		log.Fatal("Error When Opening Meta")
	}
	// panic("todo")
	rows, err := db.Query(getDistinctFileName)
	if err != nil {
		fmt.Println(err.Error())
	}
	var fn string
	var flist []string
	for rows.Next() {
		rows.Scan(&fn)
		// log.Println(fn)
		flist = append(flist, fn)

	}

	for _, str := range flist {
		rows, err = db.Query(getTuplesByFileName, str)
		if err != nil {
			fmt.Println(err.Error())
		}
		var fName string
		var fv int32
		var hi int
		var blockH string
		var hashL []string
		for rows.Next() {
			rows.Scan(&fName, &fv, &hi, &blockH)
			hashL = append(hashL, blockH)
		}

		currFileMeta := &FileMetaData{
			Filename:      fName,
			Version:       int32(fv),
			BlockHashList: hashL,
		}

		fileMetaMap[currFileMeta.Filename] = currFileMeta

	}
	return fileMetaMap, nil
}

/*
	Debugging Related
*/

// PrintMetaMap prints the contents of the metadata map.
// You might find this function useful for debugging.
func PrintMetaMap(metaMap map[string]*FileMetaData) {

	fmt.Println("--------BEGIN PRINT MAP--------")

	for _, filemeta := range metaMap {
		fmt.Println("\t", filemeta.Filename, filemeta.Version)
		for _, blockHash := range filemeta.BlockHashList {
			fmt.Println("\t", blockHash)
		}
	}

	fmt.Println("---------END PRINT MAP--------")

}
