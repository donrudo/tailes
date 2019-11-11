package elasticapi

import (
	"encoding/json"
	"fmt"
	"context"
	"github.com/olivere/elastic/v7"
	"os"
	"time"
)


type EsV5 struct {
	EsAPI
	Server *ElasticInfo
	Client *elastic.Client
	err    error
}

func UseClientV5(serverconfig *ElasticInfo) *EsV5 {
	var Api *EsV5
	Api.Server = serverconfig
	Api.Client, Api.err = elastic.NewClient(elastic.SetSniff(false), elastic.SetURL(serverconfig.SearchConfig.Url))
	return Api
}

func (Api EsV5) NextSearch() (string, timeRange) {
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

//func (Api EsV5) Run(url string, index string, query string, field string, realTime bool, bufferSize int) {
func (Api EsV5) Run() {
	version, err := Api.Client.ElasticsearchVersion(url)
	ExitOnError(err)
	perr.Println("Elasticsearch Cluster Version:", version)

	lookup := context.Background()
	exists, err := Api.Client.IndexExists(index).Do(lookup)
	ExitOnError(err)

	if !exists {
		perr.Println("Error: Not found Index ", index)
		os.Exit(1)
	}
	searchQuery := elastic.NewQueryStringQuery(query).Field(field)
	searchQuery.UseDisMax(true)
	searchQuery.AllowLeadingWildcard(false)
	results, err := Api.Client.Search().Index(index).Query(searchQuery).
		From(0).Size(bufferSize).Sort("@timestamp", false).
		Pretty(true).Do(lookup)
	ExitOnError(err)

	result, TimeRange := Api.NextString(results)
	TimeFilter := elastic.NewRangeAggregation().Gt(TimeRange.Gte)
	boolFilter := elastic.NewBoolQuery().Must(TimeFilter)
	filterQuery := elastic.NewFilterAggregation().Filter(boolFilter)

	perr.Printf("Results: %d, Index: %s, Query: %s\n", results.TotalHits(), index, query)
	fmt.Printf(result)

	for Api.Server.SearchConfig.UsesRealTime {
		time.Sleep(5 * time.Second)
		results, err = Api.Client.Search().Index(Api.Server.SearchConfig.Index).Query(filterQuery).Sort("@timestamp", true).Pretty(true).Do(lookup)
		ExitOnError(err)
		oldGte := TimeRange.Gte
		result, TimeRange = Api.NextSearch()
		if TimeRange.Gte == "" {
			//if the last request is empty, just recycle the last valid timestamp to continue the tailf-ing
			TimeRange.Gte = oldGte
		}
		TimeFilter = elastic.NewRangeAggregation().Gt(TimeRange.Gte)
		boolFilter := elastic.NewBoolQuery().Must(TimeFilter)
		filterQuery = elastic.NewFilterAggregation().Filter(boolFilter)
		fmt.Print(fmt.Sprintf(result))
	}
}

func (Api EsV5) StopSearch() {

}
