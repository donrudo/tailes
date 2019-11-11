package main

import (
	"elasticapi"
	"flag"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime/trace"
	"syscall"
)

var (
	traceFile    *os.File
	enableTrace  = flag.Bool("trace", false, "Generates a trace file for profiling purposes")
	realTime     = flag.Bool("f", false, "Causes tail to keep reading new entries from Elasticsearch")
	useTimestamp = flag.Bool("t", true, "If false, the search will won't fail if timestamp field "+
		"is not present at such index.")
	debug      = flag.Bool("debug", false, "Enables debug mode and exposes port 8080")
	url        = flag.String("url", "", "Elasticsearch endpoint to be used")
	index      = flag.String("i", "logstash-*", "Index to use to query for results")
	query      = flag.String("query", "*", "Query to be sent to Elasticsearch")
	field      = flag.String("field", "message", "Field to apply the query search to")
	bufferSize = flag.Int("n", 10, "Specifies the number of search results to be queried")
	perr       = log.New(os.Stderr, "", 0)
)

func gentlyTerminate(c chan os.Signal) {
	sig := <-c

	fmt.Println("\nsignal: ", sig)

	if *enableTrace {
		defer trace.Stop()
		defer traceFile.Close()
	}
	os.Exit(0)
}

func main() {
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	var err error
	flag.Parse()
	log.Printf("Trace is %b\n", *enableTrace)
	if *enableTrace {

		//@TODO: Change to use census instead.
		traceFile, err = os.Create("trace.out")
		if err != nil {
			panic(err)
		}
		trace.Start(traceFile)

	}
	if *debug {
		go func() {
			log.Println(http.ListenAndServe("localhost:8080", nil))
		}()
	}
	if *url == "" {
		perr.Println("Error: must specify -url ES endpoint")
		os.Exit(1)
	}

	es, err := elasticapi.NewClient(url, index, query, field, realTime, useTimestamp, bufferSize)
	if err != nil {
		perr.Println(err)
		os.Exit(1)
	}

	es.Run()
	go gentlyTerminate(c)
	for {
		select {}
	}
}
