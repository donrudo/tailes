package elasticapi

import (
	"gopkg.in/olivere/elastic.v2"
	"fmt"
	"encoding/json"
	"time"
	"os"
)

type EsV1 struct {
	Client *elastic.Client
	err error


	SearchConfig struct {
		Index string
		Query string
		Field string
		Buffersize int
	}
}

func UseClientV1(url string) (EsV1){
	var Api EsV1
	Api.Client, Api.err = elastic.NewClient(elastic.SetSniff(false), elastic.SetURL(url))
	return Api
}

func (Api EsV1) GetResultString(results *elastic.SearchResult) (string, timeRange ) {
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

func (Api EsV1) Run(url string, index string, query string, field string, realTime bool, bufferSize int) {

	version, err := Api.Client.ElasticsearchVersion(url)
	ExitOnError(err)
	perr.Println("Elasticsearch Cluster Version:", version)

	exists, err := Api.Client.IndexExists(index).Do()
	ExitOnError(err)

	if !exists {
		perr.Println("Error: Not found Index ", index)
		os.Exit(1)
	}
	searchQuery := elastic.NewQueryStringQuery(query).Field(field)
	searchQuery.UseDisMax(true)
	searchQuery.AllowLeadingWildcard(false)
	results, err := Api.Client.Search().Index(index).Query(searchQuery).From(0).Size(bufferSize).Sort("@timestamp", false).Pretty(true).Do()

	ExitOnError(err)

	result, TimeRange := Api.GetResultString(results)
	TimeFilter := elastic.NewRangeFilter("@timestamp").Gt(TimeRange.Gte)
	boolFilter := elastic.NewBoolFilter().Must(TimeFilter)
	filterQuery := elastic.NewFilteredQuery(searchQuery).Filter(boolFilter)

	perr.Printf("Results: %d, Inde: %s, Query: %s\n", results.TotalHits(), index, query)
	fmt.Printf(result)

	for realTime {
		time.Sleep(5 * time.Second)
		results, err = Api.Client.Search().Index(index).Query(filterQuery).Sort("@timestamp", true).Pretty(true).Do()
		ExitOnError(err)
		oldGte := TimeRange.Gte
		result, TimeRange = Api.GetResultString(results)
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
