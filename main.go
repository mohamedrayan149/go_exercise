package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"golang.org/x/sync/errgroup"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
)

type Recommended struct {
	Price float64 `json:"price"`
}

type Packages struct {
	Recommended Recommended `json:"recommended"`
}

type Gig struct {
	Packages Packages `json:"packages"`
}

type Response struct {
	Data []Gig `json:"gigs"`
}

type Record struct {
	query           string
	numberOfResults string
}

type QueryGigsAvg struct {
	query string
	avg   float64
}

func Parsing(parserChannel chan Record) {
	file, err := os.Open("input.csv")
	if err != nil {
		defer file.Close()
		log.Fatal(err)
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Fatal(err)
		}
	}(file)
	records, err := csv.NewReader(file).ReadAll()
	if err != nil {
		log.Fatal(err)
	}
	for _, record := range records {
		if len(record) != 2 {
			log.Println("The input not good !:", record)
			continue
		}
		parserChannel <- Record{query: record[0], numberOfResults: record[1]}
	}
	close(parserChannel)
}
func Searching(ch <-chan Record, searchChannel chan QueryGigsAvg) {

	for record := range ch {
		// request should be without spaces
		queryString := strings.ReplaceAll(record.query, " ", "")
		numberOfResults := strings.ReplaceAll(string(record.numberOfResults), " ", "")
		request := fmt.Sprintf("http://kube-lb.fiverrdev.com/go-searcher/api/v5/search/auto?"+
			"query_string=%s&page_size=%s&shuffle=false&page_ctx_id=309e5608edaa59e1eb8397c5880b625a"+
			"&user_id=104151323&user_guid=36fa0150-82f9-4240-b4c8-f97d26212dfa&currency=USD",
			queryString, numberOfResults)
		resp, err := http.Get(request)
		if err != nil {
			// log.fatal terminates the program so no need for break
			log.Fatal(err)
		}
		defer func(Body io.ReadCloser) {
			err := Body.Close()
			if err != nil {
				fmt.Println("error closing body")
			}
		}(resp.Body)
		// Read the response body
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			log.Fatal(err)
		}
		// Unmarshal the JSON into the struct
		var response Response
		err = json.Unmarshal(body, &response)
		if err != nil {
			log.Fatal(err)
		}
		// Access the deeply nested fields
		sum := 0.0
		for _, gig := range response.Data {
			sum += gig.Packages.Recommended.Price
		}
		searchChannel <- QueryGigsAvg{record.query, sum / float64(len(response.Data))}
	}
	close(searchChannel)
}
func PriceCalculator(ch chan QueryGigsAvg) {
	file, err := os.Create("output.txt")
	if err != nil {
		defer file.Close()
		log.Fatal(err)
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Fatal(err)
		}
	}(file)
	writer := csv.NewWriter(file)
	defer writer.Flush()
	for query := range ch {
		line := []string{query.query, fmt.Sprintf("%.2f", query.avg)}
		err = writer.Write(line)
		if err != nil {
			fmt.Println("Error writing record:", err)
			return
		}
	}
}

func main() {
	var group errgroup.Group
	start := time.Now()
	//Parser stage
	parserChannel := make(chan Record)
	group.Go(func() error {
		Parsing(parserChannel)
		return nil
	})
	// Searcher stage
	searchChannel := make(chan QueryGigsAvg)
	group.Go(func() error {
		Searching(parserChannel, searchChannel)
		return nil
	})
	//price calculator stage
	group.Go(func() error {
		PriceCalculator(searchChannel)
		return nil
	})

	if err := group.Wait(); err != nil {
		// Handle the error
		println("Error:", err.Error())
	} else {
		// All goroutines completed successfully
		println("All stages finished without error.")
	}
	fmt.Printf("Execution time : %s \n", time.Since(start))
}
