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
	"time"
)

func getSSTableFiles(manifestPath string) ([]string, error) {
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		if os.IsNotExist(err) {
			return []string{}, nil // no manifest yet
		}
		return nil, err
	}
	lines := strings.Split(string(data), "\n")
	var files []string
	for _, line := range lines {
		if strings.TrimSpace(line) != "" {
			files = append(files, strings.TrimSpace(line))
		}
	}
	return files, nil
}


func loadManifest(filename string) ([]string,error){
	data,err:=os.ReadFile(filename)
	if err!=nil{
		if os.IsNotExist(err){
			return []string{},nil
		}
		return nil,err
	}
	lines:=strings.Split(strings.TrimSpace(string(data)),"\n")
	return lines,nil

}


func appendToManifest(manifestPath,filename string) error{
	f, err := os.OpenFile(manifestPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	_,err=f.WriteString(filename+"\n")
	return err
}







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
//Load from SSTables from manifest

	sstFiles,err:=getSSTableFiles("manifest.txt")
	if err !=nil{
		log.Fatalf("Failed to load manifest: %v",err)
	}
	for _,file:=range sstFiles{
		data,err:=sstable.ReadSSTable(file)
		if err!=nil{
			log.Fatalf("Failed to read %s: %v",file,err)
		}
		for k,v:=range data{
			m.Put(k,v)
		}
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
	key:="abc"
	value:="xyz"

	entry:=fmt.Sprintf("%s=%s",key,value)
	if err:=w.Write([]byte(entry)); err!=nil{
		log.Fatalf("Wal write failed: %v",err)
	}
	m.Put(key,value)

	//read check
	queryKey := "car"
if val, ok := m.Get(queryKey); ok {
	fmt.Println("Recovered val from memtable:", val)
} else {
	found := false
	// Search latest to oldest SSTable (reverse order)
	for i := len(sstFiles) - 1; i >= 0; i-- {
		data, err := sstable.ReadSSTable(sstFiles[i])
		if err != nil {
			log.Printf("Failed to read SSTable %s: %v", sstFiles[i], err)
			continue
		}
		if v, ok := data[queryKey]; ok {
			fmt.Println("Recovered val from SSTable", sstFiles[i], ":", v)
			found = true
			break
		}
	}
	if !found {
		fmt.Println("Key not found in any SSTable or memtable.")
	}
}


//flush check
	if shouldFlush(m){
		sstFile := fmt.Sprintf("sstable%d.txt", time.Now().UnixNano())
		

		err:=sstable.WriteSSTable(sstFile,m.Data());
		if err!=nil{
			log.Fatalf("SSTable write failed: %v",err)
		}

		err=appendToManifest("manifest.txt",sstFile)
		if err!=nil{
			log.Fatalf("Manifest update failed: %v",err)
		}

		m=memtable.NewMemTable()
	}
}