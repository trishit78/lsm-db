package lsmdb

import (
	"fmt"
	"lsmdb/memtable"
	"lsmdb/sstable"
	"lsmdb/wal"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"
)

type DB struct {
	mu       sync.RWMutex
	memtable *memtable.MemTable
	wal      *wal.WAL
	sstables []*sstable.SSTable
	config   *Config
	stopChan chan struct{}
}

type Config struct {
	MemTableSize        int    `yaml:"mem_table_size"`
	DataDir         string `yaml:"data_dir"`
	MaxLevel        int    `yaml:"max_level"`
	LevelSize       int    `yaml:"level_size"`
	CompactInterval int    `yaml:"compact_interval"`
}


func NewDB(configPath string) (*DB, error){
	configData,err:=os.ReadFile(configPath)
	if err!=nil{
		return nil,fmt.Errorf("failed to parse config file: %v",err)
	}
	var config Config
	if err:=yaml.Unmarshal(configData,&config);  //Deserializes YAML into a Config struct
	err!=nil{
		return nil,fmt.Errorf("failed to parse config file: %v",err)
	}
	db:=&DB{
		config:&config,
		sstables: make([]*sstable.SSTable, 0),
		stopChan: make(chan struct{}),
	}


	wal,err:=wal.NewWAL(config.DataDir)
	if err!=nil {
		return nil,err
	}
	db.wal=wal

	db.memtable = memtable.NewMemTable()
	levelDir:=filepath.Join(config.DataDir,"level")
	if err:=os.MkdirAll(levelDir,0755); err!=nil{
		return nil,err
	}
	levelDirs,err:=os.ReadDir(levelDir)
	if err!=nil{
		return nil,err
	}
	for _,levelDir:=range levelDirs{
		if !levelDir.IsDir(){
			continue
		}
		level,err:=strconv.Atoi(levelDir.Name())
		if err!=nil{
			continue
		}
		sst,err:=sstable.NewSSTable(config.DataDir,level)
		if err!=nil{
			return nil,fmt.Errorf("failed to load sstable at level %d: %v",level,err)
		}
		db.sstables = append(db.sstables, sst)
	}


	entries ,err:=db.wal.Recover()
	if err!=nil{
		return nil,fmt.Errorf("failed to recover from WAL: %v",err)
	}
	for _,entry:= range entries{
		db.memtable.Put(entry.Key,entry.Value)
	}
	if db.memtable.Size() >0 && db.memtable.Size() >= db.config.MemTableSize {
		if err:=db.flushMemtable(); err!=nil{
			return nil,fmt.Errorf("failed to flush memtable after recovery: %v",err)
		}
	}
	go db.startBackgroundCompaction()
	return db,nil


}

func (db *DB) Put(key,value []byte) error{
	db.mu.Lock()
	defer db.mu.Unlock()

	if err:= db.wal.Write(key,value); err!=nil{
		return err
	}
	db.memtable.Put(key,value)

	if db.memtable.Size() >= db.config.MemTableSize{
		if err :=db.flushMemtable(); err!=nil{
			return err
		}
	}
	return nil
}


func (db *DB)Get(key []byte) ([]byte,error){
	db.mu.RLock()
	defer db.mu.RUnlock()

	if value,found :=db.memtable.Get(key); found{
		return value,nil
	}

	for i:=len(db.sstables)-1;i>=0;i--{
		if value,found :=db.sstables[i].Get(key);found{
			return value,nil
		}
	}
	return nil,nil
}


func (db *DB)Delete(key []byte) error{
	return db.Put(key,nil)
}


func (db *DB)Close()error{
	db.mu.Lock()
	defer db.mu.Unlock()

	close(db.stopChan)

	if err:=db.wal.Close(); err!=nil{
		return err
	}
	for _,sst:=range db.sstables{
		if err:=sst.Close();err!=nil{
			return err
		}
	}
	return nil
}


func (db *DB)compactLevel(level int)error{
	if level>=db.config.MaxLevel{
		return nil
	}
	levelSSTables:=make([]*sstable.SSTable,0)
	for _,sst:=range db.sstables{
		if strings.Contains(sst.GetPath(),fmt.Sprintf("level/%d",level)){
			levelSSTables = append(levelSSTables, sst)
		}
	}

	if len(levelSSTables) < db.config.LevelSize{
		return nil
	}
	merged,err:=sstable.Merge(levelSSTables,db.config.DataDir,level+1)
	if err!=nil{
		return err
	}
	for _,sst:=range levelSSTables{
		if err:=sst.Delete();err!=nil{
			return err
		}
	}
	newSSTables:=make([]*sstable.SSTable,0)
	for _,sst:= range db.sstables{
		if !strings.Contains(sst.GetPath(),fmt.Sprintf("level/%d",level)){
			newSSTables=append(newSSTables, sst)
		}
	}
	newSSTables = append(newSSTables, merged)
	db.sstables = newSSTables
	return db.compactLevel(level+1)


}

func (db *DB) flushMemtable() error{
	sst,err:=sstable.NewSSTable(db.config.DataDir,0)
	if err!=nil{
		return err
	}
	entries :=make([]sstable.Entry,0)
	it:=db.memtable.Iterator()
	for it.Next(){
		entries=append(entries, sstable.Entry{
			Key: it.Key(),
			Value: it.Value(),
		})
	}
	if err:=sst.Write(entries); err!=nil{
		return err
	}
	db.sstables=append(db.sstables, sst)

	db.memtable = memtable.NewMemTable()
	if err:= db.wal.Clear(); err!=nil{
		return err
	}
	return nil

}


func (db *DB) startBackgroundCompaction(){
	timer:=time.NewTicker(time.Duration(db.config.CompactInterval)*time.Second)

	defer timer.Stop()

	for{
		select{
		case<-timer.C:
			for level:=0;level<db.config.MaxLevel;level++{
				if err:=db.compactLevel(level);err!=nil{
					fmt.Printf("error during compaction at level %d: %v \n",level,err)
				}
			}
		case <-db.stopChan:
			return
		}
	}
}

