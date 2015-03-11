package main

import (
	"fmt"
	"testing"

	"github.comcast.com/viper-sde/gocollectd"
)

var _ = fmt.Println

func _decoratorMaker() (*Decorator, error, chan []byte, chan []byte) {
	inbound := make(chan []byte)
	outbound := make(chan []byte)

	d, err := NewDecorator(inbound, outbound)
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

func TestDecoratorParsesCollectdPacket(t *testing.T) {
	//TODO
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
	// TODO

}
func TestDecoratorSplitCollectdPacketHandlesEmptyValue(t *testing.T) {
	// TODO

}

func TestDecoratorSplitCollectdPacketHandlesMultipleValues(t *testing.T) {
	// TODO

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
