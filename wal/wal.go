package wal

import (
	"encoding/binary"
	"io"

	"os"
	"path/filepath"
	"sync"
)

type WAL struct {
	file *os.File
	mu sync.Mutex
	path string
	offset int64
}

func  NewWAL(dataDir string) (*WAL,error){
	if err := os.MkdirAll(dataDir,0755); err!=nil{
		return nil,err
	}
	path:=filepath.Join(dataDir,"wal.log")
	file , err := os.OpenFile(path,os.O_APPEND|os.O_CREATE|os.O_WRONLY,0644)
	if err != nil{
		return nil,err
	}
	info,err:=file.Stat()
	if err!=nil{
		file.Close()
		return nil,err
	}
	return &WAL{file:file,
		path:path,
		offset: info.Size(),
		},nil

}


// func (w *WAL) Write(data []byte) error{
// 	w.mu.Lock()

// 	defer w.mu.Unlock()

// 	_,err := w.file.Write(append(data,'\n'))
// 	if err!= nil{
// 		return err
// 	}
// 	return w.file.Sync()     // this will ensure durability

// }
func (w *WAL) Write(key, value []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err:= binary.Write(w.file,binary.LittleEndian,uint32(len(key))); err!=nil{
		return err
	}
	if err:= binary.Write(w.file,binary.LittleEndian,uint32(len(value))); err!=nil{
		return err
	}
	if _,err:=w.file.Write(key); err!=nil{
		return err
	}
	if _,err:=w.file.Write(value); err!=nil{
		return err
	}
	if err:=w.file.Sync(); err!=nil{
		return err
	}
	w.offset+=8+int64(len(key))+int64(len(value))
	return nil

}

func (w *WAL) Close() error{

	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Close()
}

func (w *WAL)Clear() error{
	w.mu.Lock()
	defer w.mu.Unlock()
	if err:=w.file.Close(); err!=nil{
		return err
	}
	file,err:=os.Create(w.path)
	if err!=nil{
		return err
	}
	w.file=file
	w.offset=0;
	return nil


}


func (w *WAL) Recover() ([]struct{Key,Value []byte},error){
	w.mu.Lock()
	defer w.mu.Unlock()
	if err:=w.file.Close(); err!=nil{
		return nil,err
	}
	file,err:=os.Open(w.path)
	if err!=nil{
		return nil,err
	}
	defer file.Close()

	var entries []struct{Key,Value []byte}
	offset:=int64(0)
	for{
		var keyLen uint32
		if err:=binary.Read(file,binary.LittleEndian,&keyLen); err!=nil{
			if err== io.EOF{
				break
			}
			return nil,err
		}

		var valueLen uint32
		if err:=binary.Read(file,binary.LittleEndian,&valueLen); err!=nil{
			if err== io.EOF{
				break
			}
			return nil,err
		}

		key:=make([]byte,keyLen)
		n,err:=file.Read(key)
		if err!=nil || n!=int(keyLen) {
			break
		}

		value :=make([]byte,valueLen)
		n,err =file.Read(value)
		if err!=nil || n!=int(valueLen) {
			break
		}
		entries = append(entries, struct{Key,Value []byte}{key,value})
		offset+=8+int64(keyLen)+int64(valueLen)
	}
	w.file,err= os.OpenFile(w.path,os.O_APPEND|os.O_CREATE|os.O_WRONLY,0644)
	if err!=nil{
		return nil,err
	}
	w.offset= offset
	return entries,nil


}