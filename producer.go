package main

import (
	"fmt"
	"time"

	"github.com/golang/glog"
	"github.comcast.com/viper-sde/sarama"
)

func NewProducer(decoratedTopic string, inbound chan []byte, client Client) (*Producer, error) {
	fmt.Println("Creating New Producer")
	p := &Producer{
		inbound: inbound,
		topic:   decoratedTopic,
		errors:  make(chan error),
	}
	var err error
	p.producer, err = sarama.NewProducerFromClient(client.(*sarama.Client))
	return p, err
}

type Producer struct {
	inbound chan []byte
	errors  chan error

	err    error
	topic  string
	client Client

	producer *sarama.Producer
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
			//default:
			//		time.Sleep(5 * time.Millisecond)
		}
	}

}

func (p *Producer) watchProducer() {
	for {
		select {
		case _ = <-p.producer.Successes():
		case msg := <-p.producer.Errors():
			glog.Error(msg)
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
}
