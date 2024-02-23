package main

import (
	"bufio"
	"fmt"
	"log"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

var result []string
var cities map[string][]float64

var mutex sync.Mutex
var wg sync.WaitGroup

var count int = 0
var ch chan string = make(chan string, 100)

func main() {
	startTime := time.Now()

	cities = make(map[string][]float64)

	//Read the file
	go func() {
		file, err := os.Open("/Users/shyam/Documents/Work/1brc/measurements.txt")
		// file, err := os.Open("./test.txt")

		if err != nil {
			log.Fatal("Could not read the file")
		}
		defer file.Close()
		scanner := bufio.NewScanner(file)

		result = []string{}
		for scanner.Scan() {
			ch <- scanner.Text()
		}

		close(ch)
	}()

	for v := range ch {
		processRow(v)
	}

	fmt.Println("Time taken In Reading the File", (time.Now().UnixMilli()-startTime.UnixMilli())/1000)
	fmt.Printf("Read total %d lines\n", count)

	// step 2 now process the data
	// fmt.Println(cities)
	for k, temps := range cities {
		wg.Add(1)
		go processCityTemp(k, temps)
	}

	wg.Wait()

	fmt.Println(strings.Join(result, ", "))

	endTime := time.Now()
	fmt.Println("Time taken", (endTime.UnixMilli()-startTime.UnixMilli())/1000)

}

func processRow(data string) {
	// fmt.Println(data)
	count++
	row := strings.Split(data, ";")
	rowTemp, _ := strconv.ParseFloat(row[1], 64)

	_, ok := cities[row[0]]

	if ok {
		cities[row[0]] = append(cities[row[0]], rowTemp)
	} else {
		cities[row[0]] = []float64{rowTemp}
	}
}

func processCityTemp(name string, temps []float64) {

	sort.Float64s(temps)

	sum := 0.0
	for _, v := range temps {
		sum += v
	}
	avg := sum / float64(len(temps))
	avg = math.Ceil(avg*10) / 10

	mutex.Lock()
	defer mutex.Unlock()
	result = append(result, fmt.Sprintf("%s=%.1f/%.1f/%.1f", name, temps[0], avg, temps[len(temps)-1]))
	wg.Done()
}
