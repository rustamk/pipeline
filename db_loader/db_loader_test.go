package main

import (
    "testing"
    "fmt"
)

func TestDBLoader(t *testing.T) {
    Initialize()
    rowsProcessed := RunProcessing(false)
    if rowsProcessed < 10 {
        t.Error("Test FAILED - less than 10 rows found. Only", rowsProcessed, "were processed")
    } else {
        fmt.Println("Test SUCCEED - data process count", rowsProcessed)
    }
}

