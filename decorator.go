package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/golang/glog"

	"github.comcast.com/viper-sde/gocollectd"
)

// Creates a new decorator
func NewDecorator(inbound chan []byte, outbound chan []byte) (*Decorator, error) {

	cache := make(map[string]Packet)
	glog.Warning("Preheating cache with bullshit map.")
	cache["xxx"] = Packet{
		"cluster":   "a",
		"esxi_host": "10.22.222.2",
		"docker":    true,
		"virtual":   true,
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

	// TODO:  make the cache hold typed data with a configurable TTL
	cache map[string]Packet
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
			a := time.Now()
			if packets, err := d.parseCollectdPacket(msg); err != nil {
				glog.Error(err)
			} else {
				// TODO:  this will leak memory if the decorator queue isn't being read.
				go d.send(packets)
			}
			glog.Info("parseCollectdPacket completed in ", time.Now().Sub(a))

		default:
			sleep := 250 * time.Millisecond
			glog.Info("No inbound packets.  Sleeping ", sleep)
			time.Sleep(sleep)
		}
	}
}

func (d *Decorator) send(packets [][]byte) {
	for _, packet := range packets {
		glog.Info("sending packet")
		d.outbound <- packet
	}
}

type Packet map[string]interface{}

// Duplicates the contents of this packet into
func (p Packet) Copy() Packet {
	dst := Packet{}
	for key, value := range p {
		dst[key] = value
	}
	return dst
}

// Given an array of collectd.Packet objects, extracts the hostname and returns
// a Packet containing the dimensional data string for that hostname.
func (d *Decorator) getHostDimensions(packets *[]gocollectd.Packet) Packet {
	if len(*packets) == 0 {
		return Packet{
			"error": errors.New("Collectd packets empty"),
		}
	}

	p := *packets
	if meta, err := d.getHostData(p[0].Hostname); err == nil {
		return meta
	} else {
		return Packet{
			"error": err,
		}
	}
}

// Splits a collectd packet out into its constitutent messages.
func (d *Decorator) parseCollectdPacket(b []byte) ([][]byte, error) {
	var packets *[]gocollectd.Packet
	var err error
	if packets, err = gocollectd.Parse(b); err != nil {
		return [][]byte{}, err
	}

	dimensions := d.getHostDimensions(packets)

	for _, packet := range *packets {
		fmt.Println("================================================================================")

		//TODO:  this needs to be a pile of goroutines, not a synchronous operation?

		// searching cache for host match.
		// If not found, searching remote API.
		// Build map[string]interface{} of collectd information
		glog.Infof("Retrieved metadata for %s: [%s]", packet.Hostname, dimensions)
		collectd := dimensions.Copy()
		collectd["hostname"] = packet.Hostname
		collectd["timestamp"] = packet.TimeUnix()
		collectd["plugin"] = packet.Plugin
		collectd["plugin_instance"] = packet.PluginInstance
		collectd["type"] = packet.Type
		collectd["type_instance"] = packet.TypeInstance
		collectd["name"] = packet.Name()

		fmt.Println("printing initial collectd packet")
		n, _ := json.Marshal(collectd)
		fmt.Println(string(n))

		valueNames := packet.ValueNames()
		for i, val := range packet.Values() {
			fmt.Println("Printing ", i)
			var num gocollectd.Number
			var err error
			if num, err = val.Number(); err != nil {
				glog.Error(err)
				continue
			}
			collectd2 := collectd.Copy()
			collectd2["metric"] = valueNames[i]
			collectd2["value"] = num.Float64()
			collectd2["series_type"] = num.CollectdType()
			n, err := json.Marshal(collectd2)
			fmt.Println(string(n))
		}
		n, _ = json.Marshal(collectd)
		fmt.Println(string(n))
		//val, _ := packet.Values()[0].Number()
		//glog.Info(i, packet.Hostname, packet.Name(), packet.TimeUnix(), packet.Plugin, packet.PluginInstance,
		//packet.Type, packet.TypeInstance, packet.ValueNames(), val)

		// join the collectd information with the dimensions information and serialize to JSON, then to bytes..
	}
	return [][]byte{}, nil
}

// retrieves decoration string from the decorator's local cache.
// Entries in the cache have a TTL of 5 minutes +- 150 seconds, after which the record
// for the hostname will expire and the query will return an error.
func (d *Decorator) getHostData(hostname string) (Packet, error) {
	if match, ok := d.cache[hostname]; ok == false {
		glog.Info("Cache miss.  Retrieving metadata from remote source")
		return d.getRemoteHostData(hostname)
	} else {
		glog.Info("Cache hit.  Returning metadata.")
		return match, nil
	}
}

// Retrieves decoration string from a remote API.
func (d *Decorator) getRemoteHostData(hostname string) (Packet, error) {
	resp, err := http.Get(fmt.Sprintf(decoratorHost, hostname))
	if err != nil {
		glog.Error("Decorator HTTP request", err)
		return nil, err
	}
	defer resp.Body.Close()

	// Validating HTTP response is good
	if resp.StatusCode != 200 {
		glog.Error("Decorator HTTP response", resp.StatusCode)
		return nil, errors.New("Bad response from decorator")
	}

	// Reading the response body
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		glog.Error("Decorator response read", err)
		return nil, err
	}

	// Unmarshaling the response JSON into the output variable
	decoration := Packet{}
	err = json.Unmarshal(body, &decoration)
	if err != nil {
		glog.Error("Decorator response unmarshal", err)
		return nil, err
	}

	// Priming the cache for next iteration.
	d.cache[hostname] = decoration

	glog.Info("Successfully retrieved data", decoration)
	return decoration, nil
}
