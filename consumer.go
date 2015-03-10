package main

import (
	"errors"
	"fmt"
	"time"

	"github.comcast.com/viper-sde/sarama"
)

var _ = fmt.Println

// stub wrapper around sarama.Client
type Client interface {
	Partitions()
	ConsumePartition()
	GetOffset()
}

// Wrapper around sarama.PartitionConsumer
type PartitionConsumer interface {
	Messages() <-chan *sarama.ConsumerMessage
	Errors() <-chan *sarama.ConsumerError
	Close() error
	AsyncClose()
}

// Creates a new Consumer with the given topic and client.
func NewConsumer(topic string, outboundChan chan []byte, client *sarama.Client) (*Consumer, error) {
	errors := make(chan error)
	var c *Consumer = &Consumer{
		errors:   errors,
		outbound: outboundChan,
		client:   client,
		topic:    topic,
	}

	var err error
	c.consumer, err = sarama.NewConsumerFromClient(client)
	return c, err
}

// Consumer establishes a connection to Kafka and reads messages off of the queue, pushing them into Messages
type Consumer struct {
	errors   chan error
	outbound chan []byte

	err    error
	topic  string
	client *sarama.Client

	consumer          *sarama.Consumer
	partitionConsumer *sarama.PartitionConsumer
}

// Returns an unbuffered chan of errors through which errors can propagate.
func (c *Consumer) Errors() chan error {
	return c.errors
}

// Returns an unbuffered chan of raw message bytes.
func (c *Consumer) Messages() chan []byte {
	return c.outbound
}

// Kicks off a process to read data from kafka and write messages to an outbound queue.
// This piece needs to kick off a partitionConsumer for each partition on the client,
// and keep a record of all of the partitionConsumers that are running at any given time,
// so that dead consumers can be restarted, and so that new partitions can be added.
func (c *Consumer) Start() {

	// Determine the number of Kafka partitions we need to read from.
	partitions, err := c.client.Partitions(c.topic)
	if err != nil {
		c.err = err
		return
	}

	// Initializing a partition consumer for each partition.  Kafka only supports adding partitions at runtime.
	// If the number of Kafka partitions is changed during runtime, this will chug along unaffected, but
	// data pushed into the new partitions will not be ingested.  The process will need to be restarted.
	for partition := range partitions {
		p := int32(partition)
		offset, _ := c.client.GetOffset(c.topic, p, sarama.LatestOffsets)
		partitionConsumer, _ := c.consumer.ConsumePartition(c.topic, p, offset)
		go partitionListener(partitionConsumer, c.errors, c.outbound)
	}
}

// Retrieves messages from the partition consumer's messages chan and publishes bytes through rawCollectd.
// On error, propagates the error to the creator via the errors channel.
func partitionListener(pc PartitionConsumer, errChan chan<- error, rawCollectd chan<- []byte) {
	for {
		select {
		case msg := <-pc.Messages():
			rawCollectd <- msg.Value
		case msg := <-pc.Errors():
			errChan <- errors.New(msg.Error())
			return
		default:
			time.Sleep(500 * time.Millisecond)
		}
	}
}
