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
	defer file.Close()
	if err != nil {
		log.Fatal(err)
		file.Close()
	}
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
		// Unmarshal into a generic map
		var result map[string]interface{}
		err = json.Unmarshal(body, &result)
		if err != nil {
			log.Fatal(err)
		}

		if data, ok := result["gigs"].([]interface{}); ok {
			sum := 0.0
			for _, item := range data {
				gig := item.(map[string]interface{})
				packages := gig["packages"].(map[string]interface{})
				recommended := packages["recommended"].(map[string]interface{})
				// the price type must be like the dynamic map
				price := recommended["price"].(float64)
				sum += price
			}
			searchChannel <- QueryGigsAvg{record.query, sum / float64(len(data))}
		}
	}
	close(searchChannel)
}
func PriceCalculator(ch chan QueryGigsAvg) {
	file2, err := os.Create("output.txt")
	if err != nil {
		log.Fatal(err)
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Fatal(err)
		}
	}(file2)
	writer := csv.NewWriter(file2)
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

/////////////////////
