package elasticapi

import (
	"net/http"
	"encoding/json"
	"strings"
	"errors"
	"log"
	"os"
)

var Info	  ElasticInfo
var perr       = log.New(os.Stderr, "", 0)

const ESV1 = "1."
const ESV2 = "2."
const ESV5 = "5."

type EsAPI interface {
	Run( string,  string, string, string, bool, int)
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
}

func ExitOnError(err error) {
	if err != nil {
		perr.Println("Error: ", err.Error())
		os.Exit(1)
	}
}

/*func ClusterVersion(url string)(string, error){
	Info, _ = GetElasticInfo(url)
	if strings.HasPrefix(Info.Version.Number, ESV1) {
		return ESV1, nil
	} else
	if strings.HasPrefix(Info.Version.Number, ESV2) {
		return ESV2, nil
	} else
	if strings.HasPrefix(Info.Version.Number, ESV5) {
		return ESV5, nil
	}
	return nil, error("Version not compatible or malformed URL")
}*/

func NewClient(url string)(EsAPI, error){
	//elastic.SetSniff(false), elastic.SetURL(url)
	Info, err := GetElasticInfo(url)
	if err != nil {
		perr.Println(err.Error())
	}
	if strings.HasPrefix(Info.Version.Number, ESV1) {
		Client := UseClientV1(url)
		return Client, nil
	} else
	if strings.HasPrefix(Info.Version.Number, ESV5) {
		Client := UseClientV5(url)
		return Client, nil
	} /*else
	if strings.HasPrefix(Info.Version.Number, ESV2) {
		Client := UseClientV2(url)
		return Client, nil
	}*/

	return nil, errors.New(Info.Version.Number + " Version not compatible or malformed URL")
}

/**
 *	GetElasticInfo from the given url and returns it to be used by the application.
 */
func GetElasticInfo(url string) (ElasticInfo, error){

	var body ElasticInfo

	perr.Println(url)
	res, err := http.Get(url)
	if err != nil {
		return body, err
	}
	err = json.NewDecoder(res.Body).Decode(&body)
	return body, err

}
