package memtable

import "sync"

type MemTable struct {
	mu sync.RWMutex
	table map[string] string
}

func NewMemTable() *MemTable{
	return &MemTable{
		table:make(map[string]string),
	}
}

func (m *MemTable) Put(key,value string){
	m.mu.Lock()      //locks the transaction
	defer m.mu.Unlock()        // transaction will unlock at any point of time(after completion of the function)
	m.table[key] = value
}

func (m *MemTable) Get(key string) (string,bool){
	m.mu.RLock()   // locks only for reading 
	defer m.mu.RUnlock()
	val, ok:= m.table[key]
	return  val,ok
}

func (m *MemTable) Delete(key string){
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.table,key)
}

