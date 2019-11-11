BROKEN PROJECT
======
I stopped the maintenance of this project for long time due to lack of time to develop it.

The new folder structure is based on the proposal at https://github.com/golang-standards/project-layout and some snippets are taken from https://github.com/digitalocean/doctl  (specially the Makefile) 


tailes
=======

Tail tool to query elasticsearch endpoints, autodetects the elasticsearch version to decide which library to use in the background.

Build
-----
The build process requires gb ( http://getgb.io ) (and hence golang too), just do:

```
gb vendor restore
gb build
```

Usage
-----
```
$ tailes --help

Usage of ./tailes-linux-amd64:
  -debug
    	Enables debug mode and exposes port 8080
  -f	Causes tail to keep reading new entries from Elasticsearch
  -field string
    	Field to apply the query search to (default "message")
  -i string
    	Index to use to query for results (default "logstash-*")
  -n int
    	Specifies the number of search results to be queried (default 10)
  -query string
    	Query to be sent to Elasticsearch (default "*")
  -url string
    	Elasticsearch endpoint to be used
```
