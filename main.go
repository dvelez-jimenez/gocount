package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"sync"
)

type stats struct {
	fileName string
	isLoad   bool
	loads    int
	inserts  int
	updates  int
	deletes  int
}

func main() {

	args := os.Args[1:]

	dir := args[0]
	if dir == "" {
		dir = "."
	}
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		log.Fatal(err)
	}
	var wg sync.WaitGroup
	input := make(chan stats)

	for _, file := range files {
		if !file.IsDir() {
			fmt.Println("Reading ", file.Name())
			fileName := file.Name()
			wg.Add(1)
			go doTheThing(dir+"/"+fileName, &wg, &input)
		}
	}

	go func(wg *sync.WaitGroup, messages chan stats) {
		wg.Wait()
		close(messages)
	}(&wg, input)

	total := 0
	inserts := 0
	updates := 0
	deletes := 0
	loads := 0
	//Range over the channel until is closed
	for stat := range input {
		if stat.isLoad {
			total += stat.loads
			loads += stat.loads
		} else {
			inserts += stat.inserts
			updates += stat.updates
			deletes += stat.deletes
			total += (stat.inserts - stat.deletes)
		}
	}

	fmt.Println(dir)
	fmt.Println("loads,", loads)
	fmt.Println("inserts,", inserts)
	fmt.Println("updates,", updates)
	fmt.Println("deletes,", deletes)
	fmt.Println("inserts,", inserts)
	fmt.Println("total,", total)
	//close(input)
}

func doTheThing(fileName string, wg *sync.WaitGroup, ch *chan stats) {
	defer wg.Done()
	*ch <- countLines(fileName)
}

func countLines(fileName string) stats {

	file, err := os.Open(fileName)
	fileInfo, _ := file.Stat()
	fName := fileInfo.Name()

	isLoad := strings.HasPrefix(fName, "LOAD")

	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		if err = file.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	file_reader := bufio.NewReader(file)

	csv_reader := csv.NewReader(file_reader)

	loads := 0
	inserts := 0
	deletes := 0
	updates := 0

	for {

		r, err := csv_reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Println(fName, err)
		}
		if isLoad {
			loads = loads + 1
		} else {
			op := r[0]

			if op == "I" {
				inserts = inserts + 1
			} else {
				if op == "D" {
					deletes = deletes + 1
				} else {
					if op == "U" {
						fmt.Println(fName, ": ", r)
						updates = updates + 1
					}
				}
			}

		}
		//fmt.Println(record)
	}
	var stat = stats{fName, isLoad, loads, inserts, updates, deletes}
	return stat
}
