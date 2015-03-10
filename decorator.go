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

	cache := make(map[string]map[string]interface{})
	glog.Warning("Preheating cache with bullshit map.")
	cache["xxx"] = map[string]interface{}{
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
	cache map[string]map[string]interface{}
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

	var meta map[string]interface{}
	for i, packet := range *packets {

		//TODO:  this needs to be a pile of goroutines, not a synchronous operation?

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
func (d *Decorator) GetHostData(hostname string) (map[string]interface{}, error) {
	if match, ok := d.cache[hostname]; ok == false {
		glog.Info("Cache miss.  Retrieving metadata from remote source")
		return d.GetRemoteHostData(hostname)
	} else {
		glog.Info("Cache hit.  Returning metadata.")
		return match, nil
	}
}

// Retrieves decoration string from a remote API.
func (d *Decorator) GetRemoteHostData(hostname string) (map[string]interface{}, error) {
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
	decoration := map[string]interface{}{}
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
