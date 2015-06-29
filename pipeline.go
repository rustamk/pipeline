package main

import (
	"flag"
	"fmt"
	"time"

	"github.com/golang/glog"
	"gopkg.in/Shopify/sarama.v1"
)

var _ = fmt.Println

func run(config *Config) {

	saramaConfig := sarama.NewConfig()
	saramaConfig.ClientID = config.Kafka.ClientId

	client, err := sarama.NewClient(config.Kafka.Servers, saramaConfig)
	if err != nil {
		panic(err)
	}

	cRaw := make(chan []byte, config.ChannelSize)
	cDecorated := make(chan []byte, config.ChannelSize)

	consumer, err := NewConsumer(config, cRaw, client)
	if err != nil {
		panic(err)
	}

	decorator, err := NewDecorator(config, consumer.Messages(), cDecorated)
	if err != nil {
		panic(err)
	}

	producer, err := NewProducer(config, decorator.Messages(), client)
	if err != nil {
		panic(err)
	}

	consumer.Start()
	decorator.Start()
	producer.Start()

	start := time.Now()
	for {
		glog.Infof("Running %v", time.Now().Sub(start))
		time.Sleep(5 * time.Second)
	}

}

func main() {
	flag.Set("logtostderr", "true")

	config := GetConfig()
	run(config)
}
