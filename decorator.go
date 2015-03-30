package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"time"

	"github.com/golang/glog"

	"github.comcast.com/viper-sde/gocollectd"
)

// Creates a new decorator
func NewDecorator(config *Config, inbound chan []byte, outbound chan []byte) (*Decorator, error) {
	cache := make(map[string]Packet)
	return &Decorator{
		config:   config,
		inbound:  inbound,
		outbound: outbound,
		cache:    cache,
	}, nil
}

type Decorator struct {
	config *Config

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
	go d.readInbound()
}

func (d *Decorator) readInbound() {
	for {
		select {
		case msg := <-d.inbound:
			a := time.Now()
			if packets, err := d.parseCollectdPacket(msg); err != nil {
				glog.Error(err)
			} else {
				d.send(packets)
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
	glog.Infof("Received %d processed packets", len(packets))
	// TODO: remove skipped
	l := len(packets)
	sent, skipped := 1, 0
	for _, packet := range packets {
		select {
		case d.outbound <- packet:
			sent++
		}
	}
	glog.Info(len(d.outbound), " slots in outbound queue.")
	glog.Infof("Sent %d/%d", sent, l)
	glog.Infof("Skipped %d/%d", skipped, l)
}

// TODO: pull to top; add comment
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

// Takes a collectd packet and outputs one or more fully-formed Packet
// objects, depending on the count of values within the source packet.
func (d *Decorator) splitCollectdPacket(dimensions Packet, packet gocollectd.Packet) (*[]Packet, error) {
	valueCount := packet.ValueCount()
	if valueCount <= 0 {
		return &[]Packet{}, errors.New("No values present in collectd packet.")
	}

	// Building base packet.  This is the information that is common to each packet.
	//  TODO:  use constants here.
	collectd := dimensions.Copy()
	collectd["hostname"] = packet.Hostname
	collectd["timestamp"] = packet.TimeUnix()
	collectd["plugin"] = packet.Plugin
	collectd["plugin_instance"] = packet.PluginInstance
	collectd["type"] = packet.Type
	collectd["type_instance"] = packet.TypeInstance
	collectd["name"] = packet.Name()

	// Packets can have >1 value - for instance, load average (1,5,15), network (tx,rx)
	// In these cases, we split the packet into multiple packets and return both.
	packets := make([]Packet, valueCount)
	valueNames := packet.ValueNames()
	values := packet.Values()
	for i := 0; i < valueCount; i++ {
		var num gocollectd.Number
		var err error
		if num, err = values[i].Number(); err != nil {
			glog.Error(err)
			// TODO:  this is returning an orphaned error with no metadata.  should it extend the existing packet?
			packets[i] = Packet{"error": err}
			continue
		}
		p := collectd.Copy()
		p["metric"] = valueNames[i]
		p["value"] = num.Float64()
		p["series_type"] = num.CollectdType()
		packets[i] = p
	}
	return &packets, nil
}

// Splits a collectd packet out into its constitutent messages.
func (d *Decorator) parseCollectdPacket(b []byte) ([][]byte, error) {

	var packets *[]gocollectd.Packet
	var err error
	if packets, err = gocollectd.Parse(b); err != nil {
		return [][]byte{}, err
	}

	// Retrieving dimensions from either of the local cache, or the remote host.
	// These are returned in the form of a Packet struct.  If the Packet has an
	// "error" key, this indicates that we were unable to retrieve metadata.
	// This shouldn't stop continued processing of the data.
	dimensions := d.getHostDimensions(packets)
	if err, ok := dimensions["error"]; ok {
		glog.Error("Error attempting to retrieve dimensions ", err)
	} else {
		glog.Infof("Retrieved metadata ")
	}

	// Since packets can contain multiple values, we walk the packets array
	// prior to processing in order to retrieve a complete packetCount for use
	// in output array allocation.
	packetCount := 0
	for _, packet := range *packets {
		packetCount += packet.ValueCount()
	}

	// Walking the collectd packets, appending metadata, serializing, and
	// depositing into output context.
	i := 0
	output := make([][]byte, packetCount)
	for _, packet := range *packets {
		packets, _ := d.splitCollectdPacket(dimensions, packet)
		for _, packet := range *packets {
			packetBytes, err := json.Marshal(packet)
			if err != nil {
				glog.Error(err)
				i += 1
			}
			// TODO:  remove preallocated array in favor of runtime append.
			output[i] = packetBytes
			i += 1
		}
	}
	return output, nil
}

// retrieves decoration string from the decorator's local cache.
// Entries in the cache have a TTL of 5 minutes +- 150 seconds, after which the record
// for the hostname will expire and the query will return an error.
func (d *Decorator) getHostData(hostname string) (Packet, error) {
	if len(hostname) == 0 {
		return Packet{}, errors.New("0-byte hostname provided")
	}

	// QA Component - This piece returns a randomized server name
	// for each request, using ServerCount as a direction.
	if d.config.QA.Enabled {
		glog.Infof("Randomization active for hostdata. %d random hosts.", d.config.QA.ServerCount)
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		hostname = fmt.Sprintf("random_%d", r.Intn(d.config.QA.ServerCount))
	}

	glog.Infof("host: %s, cache size: %d", hostname, len(d.cache))
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
	url := fmt.Sprintf(d.config.Decorator.GetHostString(), hostname)
	resp, err := http.Get(url)
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

	// TODO:  detect empty Packet and return an error.

	// Priming the cache for next iteration.
	d.cache[hostname] = decoration

	glog.Info("Successfully retrieved data", decoration)
	return decoration, nil
}
