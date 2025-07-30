package sstable

import (
	"bufio"
	"fmt"
	"os"
	"sort"
	"strings"
	_"text/scanner"
)

func WriteSSTable(filename string, data map[string]string) error {
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	keys:=make([]string,0,len(data))
	for k:=range data{
		keys=append(keys, k)
	}
	sort.Strings(keys)
	writer:=bufio.NewWriter(f)
	for _,k:= range keys{
		line:=fmt.Sprintf("%s=%s \n",k,data[k])
		_,err:= writer.WriteString(line)
		if err !=nil{
			return err
		}
	}
	return writer.Flush()

}

func ReadSSTable(path string) (map[string]string,error){
	file, err:= os.Open(path)
	if err!=nil{
		return nil,err
	}
	defer file.Close()
	result:=make(map[string]string)
	scanner:=bufio.NewScanner(file)
	for scanner.Scan(){
		line:=scanner.Text()
		parts:=strings.SplitN(line,"=",2)
		if len(parts)==2{
			result[parts[0]]=parts[1]
		}
	}

	if err:=scanner.Err(); err!=nil{
		return nil,err
	}

	return result,nil

}