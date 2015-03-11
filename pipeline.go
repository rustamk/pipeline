package main

import (
	"flag"
	"fmt"
	"time"

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

	cRaw := make(chan []byte, 100)
	cDecorated := make(chan []byte, 100)

	consumer, err := NewConsumer(rawTopic, cRaw, client)
	if err != nil {
		panic(err)
	}

	decorator, err := NewDecorator(consumer.Messages(), cDecorated)
	if err != nil {
		panic(err)
	}

	consumer.Start()
	decorator.Start()

	for {
		time.Sleep(1 * time.Second)
	}

}
