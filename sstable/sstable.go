package sstable

import (
	
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	
	"sync"
	_ "text/scanner"
)

type SSTable struct{
	mu sync.RWMutex
	path string 
	index map[string]int64
	dataFile *os.File
}

func (s *SSTable) loadIndex() error{
	offset:= int64(0)
	for{
		var keyLen uint32
		if err:= binary.Read(s.dataFile,binary.LittleEndian,&keyLen); err!=nil{
			if err== io.EOF{
				break
			}
			return err
		}

		var valueLen uint32
		if err:= binary.Read(s.dataFile,binary.LittleEndian,&valueLen); err!=nil{
			if err== io.EOF{
				break
			}
			return err
		}

		key:= make([]byte,keyLen)
		if _,err:=s.dataFile.Read(key);
		err!=nil{
			return err
		}
		if _,err:=s.dataFile.Read(key); err!=nil{
			return err
		}
		if _,err:=s.dataFile.Seek(int64(valueLen),1); err!=nil{
			return err
		}
		s.index[string(key)]=offset
		offset+=8+int64(keyLen)+int64(valueLen)

	}
	return  nil
}


func NewSSTable (dataDir string,level int)(*SSTable, error){
	path:=filepath.Join(dataDir,"level",fmt.Sprint(level))
	if err:=os.MkdirAll(path,0755); err!=nil{
		return nil,err
	}
	dataFilePath:= filepath.Join(path,"data.sst")
	file,err:= os.OpenFile(dataFilePath,os.O_RDWR|os.O_CREATE|os.O_APPEND,0644)
	if err!=nil{
		return nil,err
	}
	sst:=&SSTable{
		path:path,
		index: make(map[string]int64),
		dataFile: file,
	}
	info,err:=file.Stat()
	if err!=nil{
		file.Close()
		return nil,err
	}
	if info.Size()>0{
		if err:=sst.loadIndex(); err!=nil{
			file.Close()
			return nil,err
		}
	}
	return sst,nil
}



type Entry struct{
	Key []byte
	Value []byte
}

func (s *SSTable)writeEntry(entry Entry)(int64,error){
	info,err:=s.dataFile.Stat()
	if err!=nil{
		return 0,err
	}
	offset:=info.Size()

	if err:=binary.Write(s.dataFile,binary.LittleEndian,uint32(len(entry.Key))); err!=nil{
		return 0,err
	}


	if err:=binary.Write(s.dataFile,binary.LittleEndian,uint32(len(entry.Value))); err!=nil{
		return 0,err
	}

	if _,err:=s.dataFile.Write(entry.Key); err!=nil{
		return 0,err
	}
	if _,err:= s.dataFile.Write(entry.Value); err!=nil{
		return 0,err
	}
	return offset,nil	
}



func (s *SSTable) readEntry(offset int64)(Entry,error){
	if _,err:=s.dataFile.Seek(offset,0); err!=nil{
		return Entry{},err
	}
	var keyLen,valueLen uint32
	if err:= binary.Read(s.dataFile,binary.LittleEndian,&keyLen); err!=nil{
		return Entry{},err
	}
	if err:= binary.Read(s.dataFile,binary.LittleEndian,&valueLen); err!=nil{
		return Entry{},err
	}
	
	key:=make([]byte,keyLen)
	value:=make([]byte,valueLen)
	if _,err:=s.dataFile.Read(key); err!=nil{
		return Entry{},err
	}
	if _,err:=s.dataFile.Read(value); err!=nil{
		return Entry{},err
	}
	return Entry{Key: key,Value: value},nil

}

func (s *SSTable)Close() error{
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dataFile.Close()

}








func (s *SSTable) Write(entries []Entry)error{
	s.mu.Lock()
	defer s.mu.Unlock()
	sort.Slice(entries, func(i,j int)bool{
		return string(entries[i].Key)<string(entries[j].Key)
	})
	for _,entry:=range entries{
		offset,err:=s.writeEntry(entry)
		if err!=nil{
			return err
		}
		s.index[string(entry.Key)]=offset
	}
	return nil
}

func (s *SSTable)getAllEntries() ([]Entry,error){
	s.mu.RLock()
	defer s.mu.RUnlock()
	var entries []Entry
	offset:=int64(0)

	for{
		entry,err:=s.readEntry(offset)
		if err!=nil{
			if err==io.EOF{
				break
			}
			return nil,err
		}
		entries=append(entries,entry)
		offset+=8+int64(len(entry.Key))+int64(len(entry.Value))
	}
	return entries,nil

}

func (s *SSTable) Get(key []byte) ([]byte,bool){
	s.mu.RLock()
	defer s.mu.RUnlock()

	offset,found := s.index[string(key)]
	if !found{
		return nil,false
	}
	entry,err:=s.readEntry(offset)
	if err!=nil{
		return nil,false
	}
	return entry.Value,true
}


func (s *SSTable)Delete() error{
	s.mu.Lock()
	defer s.mu.Unlock()
	if err:=s.dataFile.Close(); err!=nil{
		return err
	}

	return os.RemoveAll(s.path)
}


func (s *SSTable) GetPath() string{
	return s.path
}









func Merge(sstables []*SSTable,dataDir string,level int)(*SSTable , error){
	merged,err := NewSSTable(dataDir,level)
	if err!=nil{
		return  nil,err
	}
	var allEntries []Entry
	for _,sst:=range sstables{
		entries,err:=sst.getAllEntries()
		if err!=nil{
			return nil,err
		}
		allEntries=append(allEntries, entries...)

	}

	sort.Slice(allEntries,func(i,j int) bool{
		return string(allEntries[i].Key)< string(allEntries[j].Key)
	})

	if len(allEntries)>0 {
		uniqueEntries:=[]Entry{allEntries[0]}
		for i:=1;i<len(allEntries);i++{
			if string(allEntries[i].Key) !=string(allEntries[i-1].Key){
				uniqueEntries=append(uniqueEntries, allEntries[i])
			}
		}
		allEntries=uniqueEntries
	}
	if err:=merged.Write(allEntries); err!=nil{
		return nil,err
	}
	return merged,nil
}