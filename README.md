# Kafka Processing Pipeline

This is a utility that reads data off of a Kafka topic, polls an API service to retrieve 
decoration / dimensions for the host that generated the stored metric, then publishes the
combined metric back to a different Kafka topic.


## Usage

Requires a Kafka instance and an API host that returns a decorator.  

`./pipeline`

## Runtime Flags

None.


## Configuration

Configuration is set in `config.toml`.  

**Kafka**

- client___id : sets the value of the client id that gets sent to Kafka. 
- group___id : not used at present.  sets the value of the group identity which Kafka uses to ensure all consumers of a topic do not have overlapping reads.
- raw___topic___name : the name of the topic where incoming raw data can be retrieved
- decorated___topic___name : the name of the topic where processed JSON is deposited
- servers : an array of <hostname>:<port> values for the Kafka brokers.


**Decorator**

- hostname : this is the host:port pair for the decorator API service.
- site___id : this is used to build the URL string.  the decorator at a site must be site=id aware.
- max___retries : not used.  would inform the retry logic for API http requests
- timeout___ms : not used.  would inform the maximum amount of time the decorator will wait before terminating the request.


**QA**

- enabled : determines whether server randomizer is enabled.
- server___count : determines the number of random servers to create.
