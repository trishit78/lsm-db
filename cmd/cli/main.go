// package main

// import (
// 	"bufio"
// 	"fmt"

// 	"lsmdb/memtable"

// 	"os"
// 	"strings"
// )

// func main(){

// 	mem := memtable.NewMemTable()
// 	reader := bufio.NewReader(os.Stdin)  // for buffered I/O
// 	fmt.Println("🔹 LSMDB MemTable CLI 🔹")
// 	fmt.Println("Commands: PUT <key> <value>, GET <key>, DEL <key>, EXIT")

// 	// wal , err:= wal.NewWAL("data/wal.log")
// 	// if err!= nil{
// 	// 	log.Fatal(err)
// 	// }
// 	// defer wal.Close()
// 	// wal.Write([]byte("SET key1 val1"))
// 	// wal.Write([]byte("SET key2 val2"))

// 	for{
// 		fmt.Print("> ")
// 		line,_ := reader.ReadString('\n')
// 		line = strings.TrimSpace(line)
// 		tokens:= strings.SplitN(line," ",3)

// 		if len(tokens) ==0 || tokens[0]==""{
// 			continue
// 		}
// 		switch strings.ToUpper(tokens[0]){
// 		case "PUT":
// 			if len(tokens) < 3{
// 				fmt.Println("Usage:PUT <key> <value>")
// 				continue
// 			}
// 			mem.Put(tokens[1],tokens[2])
// 			fmt.Println("ok")

// 		case "GET":
// 			if len(tokens)<2{
// 				fmt.Println("Usage:Get <key>")
// 				continue
// 			}
// 			val,ok:= mem.Get(tokens[1])
// 			if ok{
// 				fmt.Println("Value: ",val)
// 			} else{
// 				fmt.Println("key not found")
// 			}

// 		case "DEL":
// 			if len(tokens)<2{
// 				fmt.Println("Usage:DEL <key>")
// 				continue
// 			}
// 			mem.Delete(tokens[1])
// 			fmt.Println("Deleted")

// 		case "EXIT":
// 			fmt.Println("Bye.")
// 			return
// 		default:
// 			fmt.Println("Unknown command",tokens[0])

// 		}

// 	}
// }

package main

import (
	"fmt"
	"log"
	"lsmdb/memtable"
	"lsmdb/sstable"
	"lsmdb/wal"
	"os"
	"strings"
)

func shouldFlush(m *memtable.MemTable) bool {
	return len(m.Data()) >= 5 // arbitrary flush condition
}

func main(){
	w,err:= wal.NewWAL("wal.log")
	if err!=nil{
		log.Fatalf("Failed to open wal: %v",err)
	}
	defer w.Close()

	//Initialize memtable before loading SSTables or WAL
	m:= memtable.NewMemTable()
//Load from SSTable
	sstData,err:=sstable.ReadSSTable("sstable1.txt")
	if err==nil{
		for k,v:=range sstData{
			m.Put(k,v)
		}
	}else if !os.IsNotExist(err) {
		log.Fatalf("Error reading SSTable: %v",err)
	}
	

//  Replay WAL
	entries,err:= w.ReadAll()
	if err != nil{
		log.Fatalf("WAL read failed: %v",err)
	}
	for _,entry:=range entries{
		parts:=strings.SplitN(string(entry),"=",2)
		if len(parts)==2{
			m.Put(parts[0],parts[1])
		}
	}

	//manual entry
	key:="trishit"
	value:="bhowmik"

	entry:=fmt.Sprintf("%s=%s",key,value)
	if err:=w.Write([]byte(entry)); err!=nil{
		log.Fatalf("Wal write failed: %v",err)
	}
	m.Put(key,value)

	//read check
	if val,ok := m.Get("car"); ok{
		fmt.Println("Recovered val",val)
	}

//flush check
	if shouldFlush(m){
		err:=sstable.WriteSSTable("sstable1.txt",m.Data());
		if err!=nil{
			log.Fatalf("SSTable write failed: %v",err)
		}
		m=memtable.NewMemTable()
	}
}