package main

import (
	"testing"
)

func TestProducerMessage(t *testing.T) {

	i := 1
	result := getProducerMessage(i)
	if result != "world hello 1" {
		t.Error("Expected 'hello world 1'. Saw ", result)
	}

	i = 2
	result = getProducerMessage(i)
	if result != "hello world 2" {
		t.Error("Expected 'world hello 1'. Saw ", result)
	}
}
