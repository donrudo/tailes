package main

import (
	"flag"
	"github.com/donrudo/tailes/pkg/elasticapi"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
)

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

func main() {
	var err error
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

	es, err := elasticapi.NewClient(*url)
	if err != nil {
		perr.Println(err)
		os.Exit(1)
	}
	es.Run(*url, *index, *query, *field, *realTime, *bufferSize)

}
