## TODO

###Phase 2
* fix stddev/var aggregates
* better way to deal w/ limit of hdb
* double check how we handle arrays
* complete functions list 
* do old and new kdb timestamps work together?
* no user permissions in connector
* clean up patch for dremio-oss to add connector
* limit + hdb

###Phase 2
* parallelize queries - need to build a metadata store
* join push down
* have to push down 'having' correctly...fby is the ticket 
* elastic style snippet pushdown
* functions/views
* asof
* xbar improvement to allow by eg 5 min rather than just 1 min
* median/percentile, first/last, custom functions
* 0-gc c.java
* cost tuning

## May be working now: deploy to test
* test reflections + kdb
* add proper filter: only run agaisnt hdb if has limit, is sample, has partition clause (see email)
* ensure that dates and times are converted correctly 



