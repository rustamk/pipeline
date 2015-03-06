package main

import (
    "fmt"
    "strconv"
    "bufio"
    "strings"
    "time"
    "runtime"
    "path/filepath"
    "github.com/Shopify/sarama"
    "os"
    "encoding/json"
)

// ServerMetric stores 1 time series metric line from collectd.  All strings intentional - they're being sent to ingest services over http
type ServerMetric struct {
    Host string
    Collection string
    Metric string
    Time string
    Value string
}
const MIN_EPOCH_TIME_LENGTH = 14
const TIME_THRESHOLD = 50.0
const BATCH_SIZE = 10000
const MAX_BYTES_TO_LOAD int64 = 5000
const DATA_DIR = "/var/lib/collectd"
const PROCESSING_FILE_NAME = "load_info.txt"
const CLIENT_ID string = "plexus-collectd"
const KAFKA_TOPIC string = "sde-collectd"
var KAFKA_BROKERS = []string{"172.30.18.139:9092","172.30.18.140:9092"}

// This process is started from the command line main method.  Runs in an endless loop processing data deltas on the plexus server and sending to ingest
func main() {
    runtime.GOMAXPROCS(2)
    for 1==1 {
        DataCollection()
        time.Sleep(time.Second * 5)
    }
}

func DataCollection() int {
    fmt.Println("Data Collection")
    defer func() {
        if e := recover(); e != nil {
            fmt.Println("Panic Recovery. System will wait 10 seconds and retry processing.", e)
            time.Sleep(time.Second * 10)
        }
    }()

    loadedServerMap := getPreviouslyLoadedDataMap()

    // open file for writing
    processingFile,_ := os.Create(PROCESSING_FILE_NAME)

    // processMap contains all the new metrics found that need to be processed (sent to collection ingest)
    processMap := make(map[string]ServerMetric)

    rowsProcessed := 0

    // walk the entire data collection directory looking for all files
    filepath.Walk(DATA_DIR, func(path string, info os.FileInfo, err error) error {
        if (!info.IsDir()) {
            rowsProcessed += processFile(path, info, loadedServerMap, processMap, processingFile)
        }
        //exit directory walk
        return nil
    })
    if (len(processMap) > 0) {
        sendAllData(processMap)
    }
    processingFile.Sync()
    processingFile.Close()

    return rowsProcessed
}

func getPreviouslyLoadedDataMap() map[string]int64 {
    fmt.Println("getPreviouslyLoadedDataMap")
    // the PROCESSING_FILE_NAME file contains a list of every server metric file with the size of data already loaded
    // if the current file size is greater than the loaded size, we have new data to process
    if _, err := os.Stat(PROCESSING_FILE_NAME); os.IsNotExist(err) {
        // no file exists - create an empty file
        os.Create(PROCESSING_FILE_NAME)
    }

    //open file for reading
    processingFile,_ := os.Open(PROCESSING_FILE_NAME)
    defer processingFile.Close()

    // read contents into a map
    loadedServerMap := make(map[string]int64)
    scanner := bufio.NewScanner(processingFile)
    for scanner.Scan() {
        str := scanner.Text()
        values := strings.Split(str, "~")
        if (len(values) == 2) {
            key := values[0]
            value := values[1]
            loadedServerMap[key],_ = strconv.ParseInt(value, 0, 64)
        }
    }

    // Clear file so we can repopulate it with new metrics
    processingFile.Truncate(0)
    processingFile.Close()

    return loadedServerMap
}

// This function processes a collectd data metrics file.  For any new deltas found, write those deltas to a map that will later be sent fo ingest
func processFile(path string, info os.FileInfo, loadedServerMap map[string]int64, processMap map[string]ServerMetric, processingFile *os.File) int {
    now := time.Now()
    hoursSinceUpdate := now.Sub(info.ModTime()).Hours()

    rowsProcessed := 0

    if (hoursSinceUpdate < TIME_THRESHOLD) {
        values := strings.Split(path, "/")
        // directory structure is <server-name>/<collection-name>/<metric-name>
        metric := values[len(values)-1]
        collection := values[len(values)-2]
        server := values[len(values)-3]
        key := server +"|" +collection +"|" +metric
        var maxByteLoaded int64 = loadedServerMap[key]

        // compare current size to number of bytes already processed.  If size() is greater we have data to process
        if info.Size() > maxByteLoaded {
            // open the data file with the collectd metrics
            lfile, _ := os.Open(path)

            // only load up to <config> number of bytes . Don't load an entire days worth of data if the file was never processed.
            bytesDifference := info.Size() - maxByteLoaded
            if (bytesDifference > MAX_BYTES_TO_LOAD) {
                bytesDifference = MAX_BYTES_TO_LOAD
            }

            // seek file forward past the point where the previous load stopped
            arr := make([]byte, bytesDifference)
            lfile.Seek(maxByteLoaded +1, 0)
            lfile.Read(arr)
            // split file into array of strings - 1 per file line
            rows := strings.Split(string(arr), "\n")
            for i := 0; i<len(rows); i+=1 {
                row := rows[i]
                // file is ',' delimited time,value
                columns := strings.Split(row, ",")
                if len(columns) == 2 {
                    time := columns[0]
                    value := columns[1]

                    // only load data that looks property formatted - with a timestamp length that is appropriate (don't try to send partial lines)
                    if len(time) >= MIN_EPOCH_TIME_LENGTH {
                        metric := ServerMetric{Host: server, Collection: collection, Metric: metric, Time: time, Value: value}
                        processMap[string(len(processMap))] = metric
                        rowsProcessed += 1

                        // batch results that we'll push to data ingest
                        if (len(processMap) >= BATCH_SIZE) {
                            sendAllData(processMap)
                        }
                    }
                }
            }
            lfile.Close()
        }

        // write the meta data about how many bytes were in this file.  These bytes are ignored on the next data run
        newLine := fmt.Sprint(key, "~", info.Size(), "\n")
        processingFile.WriteString(newLine)
    }
    return rowsProcessed
}

func sendAllData(processMap map[string]ServerMetric) {
    fmt.Println("sendAllData")
    //sendDataToGoServer(processMap)
    sendDataToKafka(processMap)
    processMap = nil
    processMap = make(map[string]ServerMetric)
}

// Send list of server metrics to go server (temporary ingest proof of concept)
//func sendDataToGoServer(mapIn map[string]ServerMetric) string {
//    fmt.Println("sendDataToGoServer")
//    url := "http://69.252.123.113:8085/ingest"
//    jsonBytes,_ := json.Marshal(mapIn)
//    req,_ := http.NewRequest("POST", url, bytes.NewBuffer(jsonBytes))
//    req.Header.Set("Content-Type", "application/json")
//    req.Header.Set("Accept", "application/json")
//    httpClient := &http.Client{}
//    resp,_ := httpClient.Do(req)
//    defer resp.Body.Close()
    //fmt.Println("response Status:", resp.Status)
    //fmt.Println("response Headers:", resp.Header)
    //body, _ := ioutil.ReadAll(resp.Body)
//    return resp.Status
//}

// Send list of server metrics to kafka broker
func sendDataToKafka(mapIn map[string]ServerMetric) {
    fmt.Println(time.Now(), "sendDataToKafka")

    // create a list from the map object passed in
    objectList := make([]ServerMetric, len(mapIn))
    i := 0
    for _,value := range mapIn {
        objectList[i] = value
        i += 1
    }

    resultsMap := map[string][]ServerMetric{"MetricList": objectList}
    jsonbytes,_ := json.Marshal(resultsMap)

    clientConfig := sarama.NewClientConfig()
    client,_ := sarama.NewClient(CLIENT_ID, KAFKA_BROKERS, clientConfig)
    defer client.Close()

    producerConfig := sarama.NewProducerConfig()
    producer,_ := sarama.NewProducer(client, producerConfig)
    defer producer.Close()

    msg := &sarama.ProducerMessage{
        Topic: KAFKA_TOPIC,
        Key:   sarama.ByteEncoder([]byte("xxx")),
        Value: sarama.ByteEncoder([]byte(string(jsonbytes))),
    }
    producer.Input() <- msg
}
