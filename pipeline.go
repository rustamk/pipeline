package main

import (
	"errors"
	"flag"
	"fmt"
	"time"

	"github.com/golang/glog"

	"github.comcast.com/viper-sde/gocollectd"
	"github.comcast.com/viper-sde/sarama"
)

var _ = fmt.Println
var kafkaHost []string = []string{"192.168.59.103:9092"}

const (
	clientId string = "collectd-decorator"
	//groupId        string = "collectd-decorator"
	rawTopic       string = "test"
	decoratedTopic string = "test-decorated"
)

// Creates a new decorator
func NewDecorator(inbound chan []byte, outbound chan []byte) (*Decorator, error) {
	cache := make(map[string]map[string]string)
	glog.Warning("Preheating cache with bullshit map.")
	cache["93a1e476b36b"] = map[string]string{
		"cluster":   "a",
		"esxi_host": "10.22.222.2",
		"docker":    "1",
		"virtual":   "1",
		"arbuckle":  "david",
	}
	return &Decorator{
		inbound:  inbound,
		outbound: outbound,
		cache:    cache,
	}, nil
}

type Decorator struct {
	inbound  chan []byte
	outbound chan []byte

	cache map[string]map[string]string
}

// Returns a channel of decorated messages.
func (d *Decorator) Messages() chan []byte {
	return d.outbound
}

// Kicks off the decorator process.  Retrieves messages from the inbound queue, parses them,
// decorates them, and pushes them into the outbound queue.
func (d *Decorator) Start() {
	for {
		select {
		case msg := <-d.inbound:
			err := d.parseCollectdPacket(msg)
			glog.Error(err)
		default:
			sleep := 250 * time.Millisecond
			glog.Info("No inbound packets.  Sleeping ", sleep)
			time.Sleep(sleep)
		}
	}
}

// Splits a collectd packet out into its constitutent messages.
func (d *Decorator) parseCollectdPacket(b []byte) error {
	var packets *[]gocollectd.Packet
	var err error
	if packets, err = gocollectd.Parse(b); err != nil {
		return err
	}

	meta := map[string]string{}
	for i, packet := range *packets {
		// searching cache for host match.
		// If not found, searching remote API.
		if i == 0 {
			if meta, err = d.GetHostData(packet.Hostname); err != nil {
				if meta, err = d.GetRemoteHostData(packet.Hostname); err != nil {
					meta["error"] = "error retrieving data"
				}
			}
		}
		glog.Infof("Retrieved metadata for %s: [%s]", packet.Hostname, meta)
		val, _ := packet.Values()[0].Number()
		glog.Info(i, packet.Hostname, packet.Name(), packet.TimeUnix(), packet.Plugin, packet.PluginInstance,
			packet.Type, packet.TypeInstance, packet.ValueNames(), val)
	}
	return nil
}

// retrieves decoration string from the decorator's local cache.
// Entries in the cache have a TTL of 5 minutes +- 150 seconds, after which the record
// for the hostname will expire and the query will return an error.
func (d *Decorator) GetHostData(hostname string) (map[string]string, error) {
	if match, ok := d.cache[hostname]; ok == false {
		return map[string]string{}, errors.New("cache miss")
	} else {
		return match, nil
	}
}

// Retrieves decoration string from a remote API.
func (d *Decorator) GetRemoteHostData(hostname string) (map[string]string, error) {
	return make(map[string]string), nil
}

func main() {
	flag.Set("logtostderr", "true")

	config := sarama.NewConfig()
	config.ClientID = clientId

	client, err := sarama.NewClient(kafkaHost, config)
	if err != nil {
		panic(err)
	}

	cRaw := make(chan []byte)
	cDecorated := make(chan []byte)

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
