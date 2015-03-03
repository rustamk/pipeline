# Data Processing Pipeline Utilities

Here are utilities for processing raw data into some useable form.

Either in the case of reading CSV off of the Plexus and feeding it into
an external data source.

Or in the case of serving up the CSV files from disk.

Or in the case of reading raw data from Kafka and writing decorated data back
to Kafka.


## Utilities

**./server**
utility for reading static files and serving their contents as JSON

**./plexus**
utility for reading CSV data, taking a delta, and writing that data to kafka

**./decorator.go**
utility for reading from a kafka queue, decorating data using a remote API, and writing to a different queue

## Code formatting guidelines

Use `go fmt`


## Evaluating test coverage

100% coverage isn't as important as having coverage over the contact points between our application and the outside world.  

Anywhere data is prepared from an outside source into an internal representation, that's somewhere we should have a test.  
The rest can be design-by-contract, at least until we have interns to sic on the task.

```
$ go test
PASS
ok      github.comcast.com/viper-sde/pipeline   0.007s
$ go test -cover

PASS
coverage: 5.2% of statements
ok      github.comcast.com/viper-sde/pipeline   0.006s
$ go test -coverprofile=coverage.out

PASS
coverage: 5.2% of statements
ok      github.comcast.com/viper-sde/pipeline   0.008s

$ go tool cover -html=coverage.out
```


