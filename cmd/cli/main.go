package main

import (
	"bufio"
	"fmt"
	"lsmdb/memtable"
	"os"
	"strings"
)

func main(){

	mem := memtable.NewMemTable()
	reader := bufio.NewReader(os.Stdin)  // for buffered I/O
	fmt.Println("🔹 LSMDB MemTable CLI 🔹")
	fmt.Println("Commands: PUT <key> <value>, GET <key>, DEL <key>, EXIT")


	for{
		fmt.Print("> ")
		line,_ := reader.ReadString('\n')
		line = strings.TrimSpace(line)
		tokens:= strings.SplitN(line," ",3)

		if len(tokens) ==0 || tokens[0]==""{
			continue
		}
		switch strings.ToUpper(tokens[0]){
		case "PUT":
			if len(tokens) < 3{
				fmt.Println("Usage:PUT <key> <value>")
				continue
			}
			mem.Put(tokens[1],tokens[2])
			fmt.Println("ok")
		
		case "GET":
			if len(tokens)<2{
				fmt.Println("Usage:Get <key>")
				continue
			}
			val,ok:= mem.Get(tokens[1])
			if ok{
				fmt.Println("Value: ",val)
			} else{
				fmt.Println("key not found")
			}
		
		case "DEL":
			if len(tokens)<2{
				fmt.Println("Usage:DEL <key>")
				continue
			}
			mem.Delete(tokens[1])
			fmt.Println("Deleted")
		
		
		case "EXIT":
			fmt.Println("Bye.")
			return
		default:
			fmt.Println("Unknown command",tokens[0])
		
		}
	
	
	}
}
