package main

import "testing"

func _decoratorMaker() (*Decorator, error, chan []byte, chan []byte) {
	a := make(chan []byte)
	b := make(chan []byte)

	d, err := NewDecorator(a, b)
	return d, err, a, b
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
