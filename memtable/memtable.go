package memtable

import "sync"

type MemTable struct {
	mu sync.RWMutex
	table map[string][]byte
	size int
}

func NewMemTable() *MemTable{
	return &MemTable{
		table:make(map[string][]byte),
	}
}

func (m *MemTable) Put(key,value []byte){
	m.mu.Lock()      //locks the transaction
	defer m.mu.Unlock()        // transaction will unlock at any point of time(after completion of the function)
	
	keyStr:=string(key)
	oldValue :=m.table[keyStr]

	//update size
	m.size-=len(oldValue)
	m.size+=len(value)
	m.table[keyStr]= value
}

func (m *MemTable) Get(key []byte) ([]byte,bool){
	m.mu.RLock()   // locks only for reading 
	defer m.mu.RUnlock()
	val, ok:= m.table[string(key)]
	return  val,ok
}

type Iterator struct{
	keys []string
	table map[string][]byte
	cursor int
}

func (m *MemTable) Iterator() *Iterator{
	m.mu.RLock()
	defer m.mu.RUnlock()
	keys :=make([]string,0,len(m.table))
	for k:=range m.table{
		keys=append(keys, k)
	}
	return &Iterator{
		keys:keys,
		table:m.table,
		cursor:0,
	}

}


func (m *MemTable) Delete(key string){
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.table,key)
}

func (it *Iterator) Next() bool{
	it.cursor++
	return it.cursor<=len(it.keys)
}

func (it *Iterator) Key() []byte{
	return []byte(it.keys[it.cursor-1])
}

func (it *Iterator) Value() []byte{
	return it.table[it.keys[it.cursor-1]]
}

func (mt  *MemTable)Size() int{
	mt.mu.RLock()
	defer mt.mu.RUnlock()
	return mt.size
}