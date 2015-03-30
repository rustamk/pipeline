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

- client_id : sets the value of the client id that gets sent to Kafka. 
- group_id : not used at present.  sets the value of the group identity which Kafka uses to ensure all consumers of a topic do not have overlapping reads.
- raw_topic_name : the name of the topic where incoming raw data can be retrieved
- decorated_topic_name : the name of the topic where processed JSON is deposited
- servers : an array of <hostname>:<port> values for the Kafka brokers.


**Decorator**

- hostname : this is the host:port pair for the decorator API service.
- site_id : this is used to build the URL string.  the decorator at a site must be site=id aware.
- max_retries : not used.  would inform the retry logic for API http requests
- timeout_ms : not used.  would inform the maximum amount of time the decorator will wait before terminating the request.


**QA**

- enabled : determines whether server randomizer is enabled.
- server_count : determines the number of random servers to create.
