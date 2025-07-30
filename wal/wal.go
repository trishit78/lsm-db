package wal

import (
	"bytes"
	"os"
	"sync"
)

type WAL struct {
	file *os.File
	mu sync.Mutex
}

func  NewWAL(path string) (*WAL,error){
	file , err := os.OpenFile(path,os.O_APPEND|os.O_CREATE|os.O_WRONLY,0644)
	if err != nil{
		return nil,err
	}
	return &WAL{file:file},nil

}


func (w *WAL) Write(data []byte) error{
	w.mu.Lock()

	defer w.mu.Unlock()

	_,err := w.file.Write(append(data,'\n'))
	if err!= nil{
		return err
	}
	return w.file.Sync()     // this will ensure durability

}


func (w *WAL) Close() error{
	return w.file.Close()
}


func (w *WAL) ReadAll() ([][]byte,error){
	w.mu.Lock()
	defer w.mu.Unlock()
	data,err := os.ReadFile(w.file.Name())
	if err !=nil{
		return nil,err
	}
	lines :=bytes.Split(data,[]byte("\n"))

	if len(lines) >0 && len(lines[len(lines)-1]) == 0{
		lines = lines[:len(lines)-1]
	}
	return lines,nil

}
