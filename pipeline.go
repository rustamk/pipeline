package main

import (
	"flag"
	"fmt"
	"time"

	"github.com/golang/glog"
	"github.comcast.com/viper-sde/sarama"
)

var _ = fmt.Println
var kafkaHost []string = []string{"192.168.59.103:9092"}
var decoratorHost string = "http://localhost:8000/api/site/13/servers/%s/dimensions"

const (
	clientId string = "collectd-decorator"
	//groupId        string = "collectd-decorator"
	rawTopic       string = "test"
	decoratedTopic string = "test-decorated"
)

func main() {
	flag.Set("logtostderr", "true")

	config := sarama.NewConfig()
	config.ClientID = clientId

	client, err := sarama.NewClient(kafkaHost, config)
	if err != nil {
		panic(err)
	}

	cRaw := make(chan []byte, 1000)
	cDecorated := make(chan []byte, 1000)

	consumer, err := NewConsumer(rawTopic, cRaw, client)
	if err != nil {
		panic(err)
	}

	decorator, err := NewDecorator(consumer.Messages(), cDecorated)
	if err != nil {
		panic(err)
	}

	producer, err := NewProducer(decoratedTopic, decorator.Messages(), client)
	if err != nil {
		panic(err)
	}

	consumer.Start()
	decorator.Start()
	producer.Start()

	start := time.Now()
	for {
		glog.Infof("Running %v", time.Now().Sub(start))
		time.Sleep(1 * time.Second)
	}

}
