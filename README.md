tailes
=======
tailes is just a POC of a tail tool to query elasticsearch endpoints.

Build
-----
process requite gb ( http://getgb.io ) then just do:

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
