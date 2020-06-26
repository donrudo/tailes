package elasticapi

import (
	"context"
	"encoding/json"
	"fmt"
	"gopkg.in/olivere/elastic.v5"
	"os"
	"time"
)

type EsV5 struct {
	EsAPI
	Server *ElasticInfo
	Client *elastic.Client
	err    error
	ctx    context.Context
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
	searchQuery := elastic.NewQueryStringQuery(Api.Server.SearchConfig.Query).Field(Api.Server.SearchConfig.Field)
	searchQuery.UseDisMax(true)
	searchQuery.AllowLeadingWildcard(false)
	results, err := Api.Client.Search().Index(Api.Server.SearchConfig.Index).Query(searchQuery).From(0).Size(Api.Server.SearchConfig.Buffersize).Do(Api.ctx) //.Sort("@timestamp", false).Pretty(true).Do()
	if err != nil {

	}
	for _, item := range results.Hits.Hits {

		if !firstLoop {
			result = fmt.Sprintf("%s,\n", result)
		}
		firstLoop = false

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
	version, err := Api.Client.ElasticsearchVersion(Info.SearchConfig.Url)
	ExitOnError(err)
	perr.Println("Elasticsearch Cluster Version:", version)

	ctx := context.Background()
	exists, err := Api.Client.IndexExists(Info.SearchConfig.Index).Do(ctx)
	ExitOnError(err)

	if !exists {
		perr.Println("Error: Not found Index ", Info.SearchConfig.Index)
		os.Exit(1)
	}
	searchQuery := elastic.NewQueryStringQuery(Info.SearchConfig.Query).Field(Info.SearchConfig.Field)
	searchQuery.UseDisMax(true)
	searchQuery.AllowLeadingWildcard(false)
	results, err := Api.Client.Search().Index(Info.SearchConfig.Index).Query(searchQuery).
		From(0).Size(Info.SearchConfig.Buffersize).Sort("@timestamp", false).
		Pretty(true).Do(ctx)
	ExitOnError(err)

	result, TimeRange := Api.NextSearch()
	TimeFilter := elastic.NewRangeAggregation().Gt(TimeRange.Gte)
	boolFilter := elastic.NewBoolQuery().Must(TimeFilter)
	filterQuery := elastic.NewFilterAggregation().Filter(boolFilter)

	perr.Printf("Results: %d, Index: %s, Query: %s\n", results.TotalHits(), Api.Server.SearchConfig.Index, Api.Server.SearchConfig.Query)
	fmt.Printf(result)

	for Api.Server.SearchConfig.UsesRealTime {
		time.Sleep(5 * time.Second)
		results, err = Api.Client.Search().Index(Api.Server.SearchConfig.Index).Query(filterQuery).Sort("@timestamp", true).Pretty(true).Do(ctx)
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
