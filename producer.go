package main

import (
	"fmt"
	"time"

	"github.com/golang/glog"
	"gopkg.in/Shopify/sarama.v1"
)

func NewProducer(config *Config, inbound chan []byte, client Client) (*Producer, error) {
	fmt.Println("Creating New Producer")
	p := &Producer{
		config:  config,
		inbound: inbound,
		topic:   config.Kafka.DecoratedTopic,
		errors:  make(chan error),
	}
	var err error
	p.producer, err = sarama.NewAsyncProducerFromClient(client.(sarama.Client))
	return p, err
}

type Producer struct {
	config *Config

	inbound chan []byte
	errors  chan error

	err    error
	topic  string
	client Client

	producer sarama.AsyncProducer
}

func (p *Producer) Start() {
	fmt.Println("Starting Producer")
	go p.readInbound()
	go p.watchProducer()
}

func (p *Producer) readInbound() {
	fmt.Println("reading inbound")
	for {
		select {
		case msg := <-p.inbound:
			p.producer.Input() <- &sarama.ProducerMessage{
				Topic: p.topic,
				Value: sarama.ByteEncoder(msg),
			}
		}
	}

}

func (p *Producer) watchProducer() {
	t := time.NewTicker(10 * time.Millisecond)
	for {
		select {
		case _ = <-t.C:
			//do nothing.
		case _ = <-p.producer.Successes():
		case msg := <-p.producer.Errors():
			glog.Error(msg)
		}
	}
}
