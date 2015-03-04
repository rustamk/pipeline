package main

import (
    "testing"
    "os"
    "fmt"
)

// TestProcessing will call the DataCollection function to run a full plexus data process loop - using mock data written below in the test case
// The expected
func TestProcessing(t *testing.T) {
    setUpTestData()
    rowsProcessed := DataCollection()
    if rowsProcessed != 5 {
        t.Error("Test FAILED - 5 processed rows were expected but only", rowsProcessed, "were returned")
    } else {
        fmt.Println("Test SUCCEED - data process count", rowsProcessed)
    }
}

func setUpTestData() {
    os.Remove(PROCESSING_FILE_NAME)

    testServer := "plexus.test.server.data"
    testCollection := "collection01"
    testMetric := "metric01"

    serverDir := fmt.Sprint(DATA_DIR, "/", testServer)
    collectionDir := fmt.Sprint(DATA_DIR, "/", testServer, "/", testCollection)
    metricFileName := fmt.Sprint(DATA_DIR, "/", testServer, "/", testCollection, "/", testMetric)

    fmt.Println("make dir", serverDir)
    os.Mkdir(serverDir, 0777)
    os.Mkdir(collectionDir, 0777)
    metricFile,_ := os.Create(metricFileName)
    metricFile.Truncate(0)
    dataStr := "1414886322.617,28.309769\n1414886332.617,22.100006\n1414886342.617,16.899982\n1414886352.617,30.499965\n1414886362.617,28.200074\n1414886372.617,28.099891\n"
    metricFile.WriteString(dataStr)
    metricFile.Close()
}
