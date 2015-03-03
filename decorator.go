package main

import (
	"fmt"
	"time"

	"github.com/Shopify/sarama"
)

/*
   1.  read from kafka.
   2.  api calls to remote decorator source.
   3.  write to kafka.
   4.  logging everything to stdout.  steal code from pillar.
   5.  unit tests. code coverage.
   6.  error handling.
   7.  settings via toml
*/

const CLIENT_ID string = "collectd-decorator"
const KAFKA_HOST string = "192.168.59.103:9092"
const KAFKA_TOPIC string = "test"

func produce(c *sarama.Client) {

	// Initialize producer
	pc := sarama.NewProducerConfig()
	fmt.Println(pc)

	p, err := sarama.NewProducer(c, pc)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer p.Close()

	for i := 0; i < 5000; i++ {
		val := fmt.Sprintf("\"hello\":\"world %d\"", i)
		msg := &sarama.ProducerMessage{
			Topic: KAFKA_TOPIC,
			Key:   sarama.ByteEncoder([]byte("xxx")),
			Value: sarama.ByteEncoder([]byte(val)),
		}
		p.Input() <- msg
	}

	for i := 0; i < 5000; i++ {
		select {
		case msg := <-p.Errors():
			fmt.Println("error", msg.Err)
		case msg := <-p.Successes():
			fmt.Println("success", msg)
		default:
			fmt.Println("heh")
			time.Sleep(1 * time.Second)
		}
	}
}

func consume(c *sarama.Client) {

	cc := sarama.NewConsumerConfig()
	fmt.Println(cc)

	consumer, err := sarama.NewConsumer(c, cc)
	if err != nil {
		fmt.Println(err)
		return
	}

	pcc := sarama.NewPartitionConsumerConfig()
	pc, err := consumer.ConsumePartition(KAFKA_TOPIC, 0, pcc)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer pc.Close()

	i := 0
	for {
		select {
		case msg := <-pc.Messages():
			i += 1
			fmt.Println(string(msg.Key), string(msg.Value), i)
		default:
			fmt.Println("ack!")
			time.Sleep(100 * time.Millisecond)

		}
	}

}

func main() {
	fmt.Println("hello")

	// Initialize configuration
	config := sarama.NewClientConfig()
	fmt.Println(config)

	// Connect to Kafka.  Not sure why the connection name is a necessity, here.
	c, err := sarama.NewClient(CLIENT_ID, []string{KAFKA_HOST}, config)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer c.Close()
	fmt.Println(c)

	topics, err := c.Topics()
	if err != nil {
		fmt.Println(err)
		return
	}

	for i := 0; i < len(topics); i++ {
		fmt.Println(topics[i])
	}

	go produce(c)
	go consume(c)

	fmt.Println("done")
	time.Sleep(2 * time.Second)

}
