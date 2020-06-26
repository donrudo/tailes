package elasticapi

import (
	"context"
	"encoding/json"
	"fmt"
	"gopkg.in/olivere/elastic.v2"
	"os"
	"time"
)

type EsV1 struct {
	EsAPI

	Server  *ElasticInfo
	Client  *elastic.Client
	lastErr error
	Ctx     context.Context
}

func UseClientV1(serverconfig *ElasticInfo) EsV1 {
	var Api EsV1

	Api.Server = serverconfig
	Api.Client, Api.lastErr = elastic.NewClient(elastic.SetSniff(false), elastic.SetURL(serverconfig.SearchConfig.Url))

	return Api
}

func (Api EsV1) NextSearch() (string, timeRange, error) {
	var TimeRange timeRange

	searchQuery := elastic.NewQueryStringQuery(Api.Server.SearchConfig.Query).Field(Api.Server.SearchConfig.Field)
	searchQuery.UseDisMax(true)
	searchQuery.AllowLeadingWildcard(false)
	results, err := Api.Client.Search().Index(Api.Server.SearchConfig.Index).Query(searchQuery).From(0).Size(Api.Server.SearchConfig.Buffersize).Do() //.Sort("@timestamp", false).Pretty(true).Do()
	if err != nil {
		return "", TimeRange, err
	}

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
	return result, TimeRange, nil
}

func (Api EsV1) Run() {

	version, err := Api.Client.ElasticsearchVersion(Api.Server.SearchConfig.Url)
	ExitOnError(err)
	perr.Println("Elasticsearch Cluster Version:", version)

	exists, err := Api.Client.IndexExists(Api.Server.SearchConfig.Index).Do()
	ExitOnError(err)

	if !exists {
		perr.Println("Error: Not found Index ", Api.Server.SearchConfig.Index)
		os.Exit(1)
	}

	result, TimeRange, err := Api.NextSearch()
	ExitOnError(err)

	for Api.Server.SearchConfig.UsesRealTime {
		time.Sleep(5 * time.Second)
		oldGte := TimeRange.Gte
		result, TimeRange, err = Api.NextSearch()
		ExitOnError(err)
		if TimeRange.Gte == "" {
			//if the last request is empty, just recycle the last valid timestamp to continue the tailf-ing
			TimeRange.Gte = oldGte
		}

		fmt.Print(fmt.Sprintf(result))
	}
}
