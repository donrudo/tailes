package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"gopkg.in/olivere/elastic.v2"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"time"
)

type elasticSearchPrototype struct {
	Timestamp string `json:"@timestamp"`
	String    string `json:"stacktrace"`
}

type timeRange struct {
	Gte string
}

var (
	url        = flag.String("url", "", "Elasticsearch endpoint to be used")
	index      = flag.String("i", "logstash-*", "Index to use to query for results")
	realTime   = flag.Bool("f", false, "Causes tail to keep reading new entries from Elasticsearch")
	bufferSize = flag.Int("n", 10, "Specifies the number of search results to be queried")
	query      = flag.String("query", "*", "Query to be sent to Elasticsearch")
	field      = flag.String("field", "message", "Field to apply the query search to")
	perr       = log.New(os.Stderr, "", 0)
	debug      = flag.Bool("debug", false, "Enables debug mode and exposes port 8080")
)

func ExitOnError(err error) {
	if err != nil {
		perr.Println("Error: ", err.Error())
		os.Exit(1)
	}
}

func getResultString(results *elastic.SearchResult) (string, timeRange) {
	var TimeRange timeRange
	result := "["
	firstLoop := true
	var timestampOnly elasticSearchPrototype
	for _, item := range results.Hits.Hits {

		if !firstLoop {
			result = fmt.Sprintf("%s,\n", result)
		} else {
			firstLoop = false
		}

		err := json.Unmarshal(*item.Source, &timestampOnly)
		ExitOnError(err)
		TimeRange.Gte = timestampOnly.Timestamp

		raw := json.RawMessage(*item.Source)
		result = fmt.Sprintf("%s%s", result, string(raw))
	}
	result += "]"
	return result, TimeRange
}

func main() {
	flag.Parse()
	if *debug {
		go func() {
			log.Println(http.ListenAndServe("localhost:8080", nil))
		}()
	}
	if *url == "" {
		perr.Println("Error: must specify -url ES endpoint")
		os.Exit(1)
	}

	es, err := elastic.NewClient(elastic.SetSniff(false), elastic.SetURL(*url))

	version, err := es.ElasticsearchVersion(*url)
	ExitOnError(err)
	perr.Println("Elasticsearch Cluster Version:", version)

	exists, err := es.IndexExists(*index).Do()
	ExitOnError(err)

	if !exists {
		perr.Println("Error: Not found Index ", *index)
		os.Exit(1)
	}

	searchQuery := elastic.NewQueryStringQuery(*query).Field(*field)
	searchQuery.UseDisMax(true)
	searchQuery.AllowLeadingWildcard(false)

	results, err := es.Search().Index(*index).Query(searchQuery).From(0).Size(*bufferSize).Sort("@timestamp", false).Pretty(true).Do()
	ExitOnError(err)

	result, TimeRange := getResultString(results)
	TimeFilter := elastic.NewRangeFilter("@timestamp").Gt(TimeRange.Gte)
	boolFilter := elastic.NewBoolFilter().Must(TimeFilter)
	filterQuery := elastic.NewFilteredQuery(searchQuery).Filter(boolFilter)

	perr.Printf("Results: %d, Index: %s, Query: %s\n", results.TotalHits(), *index, *query)
	fmt.Printf(result)

	for *realTime {
		time.Sleep(5 * time.Second)
		results, err = es.Search().Index(*index).Query(filterQuery).Sort("@timestamp", true).Pretty(true).Do()
		ExitOnError(err)
		oldGte := TimeRange.Gte
		result, TimeRange = getResultString(results)
		if TimeRange.Gte == "" {
			//if the last request is empty, just recycle the last valid timestamp to continue the tailf-ing
			TimeRange.Gte = oldGte
		}
		TimeFilter = elastic.NewRangeFilter("@timestamp").Gt(TimeRange.Gte)
		boolFilter := elastic.NewBoolFilter().Must(TimeFilter)
		filterQuery = elastic.NewFilteredQuery(searchQuery).Filter(boolFilter)
		fmt.Print(fmt.Sprintf(result))
	}
}
