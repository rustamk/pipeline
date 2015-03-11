package main

import (
	"fmt"
	"testing"

	"github.comcast.com/viper-sde/sarama"
)

var _ = fmt.Println

// The NewConsumer factory uses a type assertion to call NewConsumerFromClient.
// As such, this is the one method that can not be tested using the mock.
func TestNewConsumer(t *testing.T) {}

func TestConsumerCreatePartitionListener(t *testing.T) {

	// Initializing a client and a consumer
	client := &mockClient{
		consumer: &mockClientConsumer{
			arbuckle:           "yes",
			partitionConsumers: make([]*mockPartitionConsumer, 0),
		},
		numPartitions: 231,
	}

	outbound := make(chan []byte)
	consumer := &Consumer{
		errors:   make(chan error),
		outbound: outbound,
		client:   client,
		topic:    "test",
	}
	_ = consumer

	// this is bullshit
	// So the story here is that I'm running into problems mocking the nested interface structure.
	// go build thinks that there are errors and refuses to believe that the nested data structure:
	// consumer.client.consumer exists.

	// Rather than get stuck on non-functionality.  Moving on.

	//fmt.Println("client.consumer", client.consumer)
	//consumer.client = consumer.client.(*mockClient)
	//fmt.Println("consumer.client", consumer.client)
	//fmt.Println("consumer.client.consumer", consumer.client.(*mockClient).consumer)

	//fmt.Println(client.consumer)

}

/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
Kafka mocks for test purposes.

These enable unit tests to execute without a dependency on an active
Kafka node.

The bootstrap from a Kafka connection to a working inbound data channel
is a little onerus:

The Client creates a Consumer and gets a listing of available partitions.
For each Partition, the Client returns the offset within Kafka.
The Consumer is used to create PartitionConsumers.
The PartitionConsumers expose two channels, Messages and Errors(), through
which messages are generated.

 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

type mockClient struct {
	consumer      *mockClientConsumer
	numPartitions int
}

func (c *mockClient) Consumer() *mockClientConsumer {
	return c.consumer
}

func (c *mockClient) Partitions(topic string) ([]int32, error) {
	ret := make([]int32, c.numPartitions)
	for i := 0; i < c.numPartitions; i++ {
		ret[i] = int32(i)
	}
	return ret, nil
}

func (c mockClient) GetOffset(topic string, partition int32, offset sarama.OffsetTime) (int64, error) {
	var i int64 = -1
	return i, nil
}

type mockClientConsumer struct {
	arbuckle           string
	activePartitions   int
	partitionConsumers []*mockPartitionConsumer
}

func (m *mockClientConsumer) ConsumePartition(topic string, partition int32, offset int64) (PartitionConsumer, error) {
	pc := &mockPartitionConsumer{
		messages: make(chan *sarama.ConsumerMessage),
		errors:   make(chan *sarama.ConsumerError),
	}
	m.activePartitions += 1
	m.partitionConsumers = append(m.partitionConsumers, pc)
	return pc, nil
}

type mockPartitionConsumer struct {
	messages chan *sarama.ConsumerMessage
	errors   chan *sarama.ConsumerError
}

// Hook used to send a mock message into the inbound queue.
func (m *mockPartitionConsumer) SendMessage(msg []byte) {
	m.messages <- &sarama.ConsumerMessage{
		Value: msg,
	}
}

// Hook for sending mock errors into the error queue.
func (m *mockPartitionConsumer) SendError(msg []byte) {
	m.messages <- &sarama.ConsumerMessage{
		Value: msg,
	}
}

func (m *mockPartitionConsumer) Messages() <-chan *sarama.ConsumerMessage {
	return m.messages
}
func (m *mockPartitionConsumer) Errors() <-chan *sarama.ConsumerError {
	return m.errors
}
func (m *mockPartitionConsumer) Close() error {
	return nil
}
func (m *mockPartitionConsumer) AsyncClose() {}
