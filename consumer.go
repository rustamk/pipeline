package main

import (
	"errors"
	"fmt"
	"time"

	"github.com/golang/glog"

	"github.comcast.com/viper-sde/sarama"
)

var _ = fmt.Println

// stub wrapper around sarama.Client
type Client interface {
	Partitions(string) ([]int32, error)
	GetOffset(string, int32, sarama.OffsetTime) (int64, error)
}

type ClientConsumer interface {
	ConsumePartition(string, int32, int64) (*sarama.PartitionConsumer, error)
}

// Wrapper around sarama.PartitionConsumer
type PartitionConsumer interface {
	Messages() <-chan *sarama.ConsumerMessage
	Errors() <-chan *sarama.ConsumerError
	Close() error
	AsyncClose()
}

// Creates a new Consumer with the given topic and client.
func NewConsumer(topic string, outboundChan chan []byte, client Client) (*Consumer, error) {
	glog.Infof("Initializing new consumer for topic [%s]", topic)
	errors := make(chan error)
	var c *Consumer = &Consumer{
		errors:   errors,
		outbound: outboundChan,
		client:   client,
		topic:    topic,
	}

	var err error
	c.consumer, err = sarama.NewConsumerFromClient(client.(*sarama.Client))
	return c, err
}

// Consumer establishes a connection to Kafka and reads messages off of the queue, pushing them into Messages
type Consumer struct {
	errors   chan error
	outbound chan []byte

	err    error
	topic  string
	client Client

	consumer ClientConsumer
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
		glog.Error(err)
		c.err = err
		return
	}

	// Initializing a partition consumer for each partition.  Kafka only supports adding partitions at runtime.
	// If the number of Kafka partitions is changed during runtime, this will chug along unaffected, but
	// data pushed into the new partitions will not be ingested.  The process will need to be restarted.
	for partition := range partitions {
		glog.Infof("Initializing partition %d at offset %d", partition, sarama.LatestOffsets)
		p := int32(partition)
		offset, _ := c.client.GetOffset(c.topic, p, sarama.LatestOffsets)
		_ = c.createPartitionListener(p, offset, c.consumer)

	}
}

func (c *Consumer) createPartitionListener(partitionId int32, offset int64, consumer ClientConsumer) error {
	if pc, err := consumer.ConsumePartition(c.topic, partitionId, offset); err != nil {
		return err
	} else {
		go partitionListener(pc, c.errors, c.outbound)
		return nil
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
			glog.Error(msg.Error())
			errChan <- errors.New(msg.Error())
			return
		default:
			time.Sleep(500 * time.Millisecond)
		}
	}
}
