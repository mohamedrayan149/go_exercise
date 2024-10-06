package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"golang.org/x/sync/errgroup"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

// Structs

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
	queryWithoutSpaces string
	numberOfResults    string
	query              string
}

type QueryGigsData struct {
	query    string
	response Response
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Functions

func Parsing(parserChannel chan Record) error {
	file, err := os.Open("input.csv")
	defer file.Close()
	defer close(parserChannel)
	records, err := csv.NewReader(file).ReadAll()
	if err != nil {
		return err
	}
	for _, record := range records {
		if len(record) != 2 {
			return nil
		}
		originalQuery := record[0]
		queryString := strings.ReplaceAll(record[0], " ", "")
		numberOfResults := strings.ReplaceAll(string(record[1]), " ", "")
		parserChannel <- Record{queryWithoutSpaces: queryString, numberOfResults: numberOfResults, query: originalQuery}
	}
	return nil
}
func Searching(ch <-chan Record, searchChannel chan QueryGigsData) error {
	defer close(searchChannel)
	for record := range ch {
		// request should be without spaces
		request := fmt.Sprintf("http://kube-lb.fiverrdev.com/go-searcher/api/v5/search/auto?"+
			"query_string=%s&page_size=%s&shuffle=false&page_ctx_id=309e5608edaa59e1eb8397c5880b625a"+
			"&user_id=104151323&user_guid=36fa0150-82f9-4240-b4c8-f97d26212dfa&currency=USD",
			record.queryWithoutSpaces, record.numberOfResults)
		resp, err := http.Get(request)
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		// Read the response body
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		// Unmarshal the JSON into the struct
		var response Response
		err = json.Unmarshal(body, &response)
		if err != nil {
			return err
		}
		searchChannel <- QueryGigsData{record.query, response}
	}

	return nil
}
func PriceCalculator(ch chan QueryGigsData) error {
	file, err := os.Create("output.txt")
	defer file.Close()
	if err != nil {
		return err
	}
	writer := csv.NewWriter(file)
	defer writer.Flush() // this ensures that the data written to the file
	for query := range ch {

		// calculation start
		sum := 0.0
		for _, gig := range query.response.Data {
			sum += gig.Packages.Recommended.Price
		}
		//
		line := []string{query.query, fmt.Sprintf("%.2f", sum/float64(len(query.response.Data)))}
		err = writer.Write(line)
		if err != nil {
			return err
		}
	}
	return nil
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func main() {
	var group errgroup.Group
	start := time.Now()
	//Parser stage
	parserChannel := make(chan Record)
	group.Go(func() error {
		return Parsing(parserChannel)

	})
	// Searcher stage
	searchChannel := make(chan QueryGigsData)
	group.Go(func() error {
		return Searching(parserChannel, searchChannel)

	})
	//price calculator stage
	group.Go(func() error {
		return PriceCalculator(searchChannel)

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
