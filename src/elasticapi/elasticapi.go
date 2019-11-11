package elasticapi

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"strings"
)

var (
	Info   ElasticInfo
	perr   = log.New(os.Stderr, "", 0)
	Client EsAPI
)

const ESV1 = "1."
const ESV2 = "2."
const ESV5 = "5."

type EsAPI interface {
	RunSearch()
	Run()
	StopSearch()
}

type elasticSearchPrototype struct {
	Timestamp string `json:"@timestamp"`
	String    string `json:"stacktrace"`
}

type timeRange struct {
	Gte string
}

type ElasticInfo struct {
	ClusterName string `json:"cluster_name"`
	Name        string `json:"name"`
	Status      int    `json:"status"`
	Tagline     string `json:"tagline"`
	Version     struct {
		BuildHash      string `json:"build_hash"`
		BuildSnapshot  bool   `json:"build_snapshot"`
		BuildTimestamp string `json:"build_timestamp"`
		LuceneVersion  string `json:"lucene_version"`
		Number         string `json:"number"`
	} `json:"version"`

	SearchConfig struct {
		Index         string
		Query         string
		Field         string
		Url           string
		UsesRealTime  bool
		UsesTimestamp bool
		Buffersize    int
	}
}

/**
ExitOnError validates any erro
*/
func ExitOnError(err error) {
	if err != nil {
		perr.Println("Error: ", err.Error())
		os.Exit(1)
	}
}

func NewClient(url, index, query, field *string,
	useRealTime, useTimestamp *bool, bufferSize *int) (EsAPI, error) {
	//elastic.SetSniff(false), elastic.SetURL(url)

	Info, err := GetElasticInfo(*url)
	if err != nil {
		perr.Println(err.Error())
	}

	Info.SearchConfig.UsesRealTime = *useRealTime
	Info.SearchConfig.UsesTimestamp = *useTimestamp
	Info.SearchConfig.Buffersize = *bufferSize
	Info.SearchConfig.Field = *field
	Info.SearchConfig.Index = *index
	Info.SearchConfig.Query = *query
	Info.SearchConfig.Url = *url

	if strings.HasPrefix(Info.Version.Number, ESV1) {
		Client := UseClientV1(&Info)
		return Client, nil
	} else if strings.HasPrefix(Info.Version.Number, ESV5) {
		Client := UseClientV5(&Info)
		return Client, nil
	}

	return nil, errors.New(Info.Version.Number + " Version not compatible or malformed URL")
}

/**
 *	GetElasticInfo from the given url and returns it to be used by the application.
 */
func GetElasticInfo(url string) (ElasticInfo, error) {

	var body ElasticInfo

	perr.Println(url)
	res, err := http.Get(url)
	if err != nil {
		return body, err
	}
	err = json.NewDecoder(res.Body).Decode(&body)
	return body, err

}
