package main

import (
	"fmt"
	"testing"

	"github.comcast.com/viper-sde/gocollectd"
)

var _ = fmt.Println

type PacketStub struct {
	Bytes           []byte
	DataTypes       []uint8
	Metric          string
	Expectation     int
	ExpectationFunc func(Packet) bool
}

var packetTests []PacketStub = []PacketStub{
	PacketStub{
		Bytes:       []byte{66, 17, 163, 99, 64, 0, 0, 0},
		DataTypes:   []uint8{1},
		Metric:      "df_etc-hostname_used",
		Expectation: 1,
	},
	PacketStub{
		Bytes:       []byte{0, 0, 0, 0, 0, 2, 153, 189, 0, 0, 0, 0, 0, 2, 153, 189},
		DataTypes:   []uint8{2, 2},
		Metric:      "if_packets_lo",
		Expectation: 2,
	},
	PacketStub{
		Bytes:       []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		DataTypes:   []uint8{2, 2},
		Metric:      "if_errors_lo",
		Expectation: 2,
	},
	PacketStub{
		Bytes:       []byte{0, 0, 0, 0, 0, 0, 0, 0},
		DataTypes:   []uint8{1},
		Metric:      "df_etc-hosts_free",
		Expectation: 1,
	},
}

var NaNPacket PacketStub = PacketStub{
	Bytes:     []byte{127, 248, 0, 0, 0, 0, 0, 0},
	DataTypes: []uint8{1},
	Metric:    "",
	ExpectationFunc: func(p Packet) bool {
		// Expects p[error] to return NaN error.
		if _, err := p["error"]; err {
			return true
		}
		return false
	},
}

func _decoratorMaker() (*Decorator, error, chan []byte, chan []byte) {
	inbound := make(chan []byte)
	outbound := make(chan []byte)
	config := GetConfig()

	d, err := NewDecorator(config, inbound, outbound)
	return d, err, inbound, outbound
}

func TestNewDecoratorPopulatesCache(t *testing.T) {
	d, err, _, _ := _decoratorMaker()
	if err != nil {
		t.Error("Unexpected error", err)
	}
	if d.cache == nil {
		t.Error("Expected decorator cache to be initialized")
	}
}

func TestDecoratorGetsHostDataFromCache(t *testing.T) {
	var d *Decorator
	var err error
	if d, err, _, _ = _decoratorMaker(); err != nil {
		t.Fatal("unexpected error retrieving cached data", err)
	}
	d.cache["arbuckle"] = Packet{
		"testing_this": true,
	}
	if _, err := d.getHostData("arbuckle"); err != nil {
		t.Fatal("unexpected error retrieving cached data", err)
	}
}

func TestDecoratorChokesOnBadHostname(t *testing.T) {
	var d *Decorator
	var err error
	if d, err, _, _ = _decoratorMaker(); err != nil {
		t.Fatal("unexpected error retrieving cached data", err)
	}
	if _, err := d.getHostData(""); err == nil {
		t.Error("Uncaught error when invalid hostname provided")
	}
}

func TestDecoratorReturnsErrorOnBadPacket(t *testing.T) {
	d := &Decorator{}
	packet := []byte("This message is invalid.")

	_, err := d.parseCollectdPacket(packet)
	if err == nil {
		t.Error("Expected breaking error when bad packet sent.")
	}
}

func TestDecoratorGetRemoteHostData(t *testing.T) {
	// TODO:  set the value of the decoratorHost constant as a field on Decorator
}

func TestDecoratorSendWritesToOutboundChan(t *testing.T) {
	d, _, _, outbound := _decoratorMaker()
	_ = outbound

	payload := [][]byte{}
	d.send(payload)
	// TODO:  write packets into d.outbound
}

func TestDecoratorGetHostDimensionsHandlesEmptyPackets(t *testing.T) {
	d, _, _, _ := _decoratorMaker()
	p := []gocollectd.Packet{}

	packet := d.getHostDimensions(&p)
	if _, ok := packet["error"]; !ok {
		t.Error("Expected Packet[errors] to be populated when empty collectd packets object sent.")
	}
	// TODO
}

func TestDecoratorSplitCollectdPacket(t *testing.T) {
	d := &Decorator{}
	dimensions := Packet{
		"one": 1,
		"two": 2,
	}
	packet := gocollectd.Packet{}

	_, err := d.splitCollectdPacket(dimensions, packet)
	if err == nil {
		t.Fatal("Expected error when empty gocollectd.Packet passed to split function.  Saw nil.")
	}

	for _, test := range packetTests {
		packet = gocollectd.Packet{
			Hostname:       "hostname",
			Plugin:         "plugin",
			PluginInstance: "pluginInstance",
			Type:           "type",
			TypeInstance:   "typeInstance",

			CdTime:     uint64(8888888888888888),
			CdInterval: uint64(1000),

			DataTypes: test.DataTypes,
			Bytes:     test.Bytes,
		}
		b, err := d.splitCollectdPacket(dimensions, packet)
		if err != nil {
			t.Fatal("Unexpected error when empty gocollectd.Packet passed to split function.  Saw ", err)
		}
		if len(*b) != test.Expectation {
			t.Fatalf("Expected %d packets returned.  Saw %d.", test.Expectation, len(*b))
		}
	}
}

func TestDecoratorSplitCollectdPacketPopulatesNaNOnError(t *testing.T) {
	d := &Decorator{}
	dimensions := Packet{
		"one": 1,
		"two": 2,
	}
	packet := gocollectd.Packet{
		Hostname:       "hostname",
		Plugin:         "plugin",
		PluginInstance: "pluginInstance",
		Type:           "type",
		TypeInstance:   "typeInstance",

		CdTime:     uint64(8888888888888888),
		CdInterval: uint64(1000),

		DataTypes: NaNPacket.DataTypes,
		Bytes:     NaNPacket.Bytes,
	}
	b, err := d.splitCollectdPacket(dimensions, packet)
	p := *b
	if err != nil {
		t.Fatal("Unexpected error parsing collectd packet.  Saw ", err)
	}
	if len(p) != 1 {
		t.Fatal("Expected NaNPacket to return only one value after split.")
	}
	if _, err := p[0]["error"]; !err {
		t.Error("Expected error to be populated in splitCollectdPacket output.")
	}
}

// Validates that the packet.Copy method returns a new copy of the key:value
// data structure.
func TestPacketCopy(t *testing.T) {
	src := Packet{
		"arbuckle": "david",
		"age":      19,
	}
	dst := src.Copy()

	if dst["arbuckle"] != "david" || dst["age"] != 19 {
		t.Fatal("Packet.Copy did not preserve values in copy operation.")
	}

	// Happy Birthday!
	dst["age"] = 20
	if src["age"] == 20 {
		t.Error("Modification to dst Packet chaned value in src packet.")
	}
}

// TODO:  some more tests:
// Decorator takes valid payload through inbound and to outbound channels
// Decorator appends internal errors to a private field when decoration fails
// Decorator accepts configuration object / API host as a dependency on creation
// Decorator doesn't block when outbound queue isn't read

// Refactoring / remaining tasks
// Clean up parseCollectdPacket.  Return all packets in an array.
// Test harness server for decorator HTTP requests
// Unit tests for inbound data.
// File-based capture of collectd streaming data for testing purposes
// Producer component.
