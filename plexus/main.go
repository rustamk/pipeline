package main

import (
    "fmt"
    "strconv"
    "bufio"
    "strings"
    "time"
    "runtime"
    "path/filepath"
    "net/http"
    "bytes"
    "github.com/jdamick/kafka"
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
const minEpochTimeLength = 14
const timeThreshold = 50.0

func main() {
    runtime.GOMAXPROCS(3)
    for 1==1 {
        dataCollection()
        time.Sleep(time.Second * 5)
    }

    //obj := ServerMetric{Host: "hostA", Collection: "collectionB", Metric: "metricC", Time: "12345", Value: "2"}
    //myMap := make(map[int]ServerMetric)
    //myMap[0] = obj
    //myMap[1] = obj
    //fmt.Println("obj ", obj)
    //sendData(myMap)
}

func dataCollection() {
    fmt.Println("start", time.Now())
    defer func() {
        if e := recover(); e != nil {
            fmt.Println("Panic Recovery... hang in there", e)
            time.Sleep(time.Second * 10)
        }
    }()

    // the load_info.txt file contains a list of every server metric file with the size of data already loaded
    // if the current file size is greater than the loaded size, we have new data to process
    fileName := "load_info.txt"
    if _, err := os.Stat(fileName); os.IsNotExist(err) {
        // no file exists - create an empty file
        os.Create(fileName)
    }
    //open file for reading
    pfile,err := os.Open(fileName)
    if err != nil {
        fmt.Println("Error opening file for read", fileName, err)
        return
    }
    defer pfile.Close()
    // read contents into a map
    loadedServerMap := make(map[string]int64)
    scanner := bufio.NewScanner(pfile)
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
    pfile.Truncate(0)
    pfile.Close()

    // open file for writing
    pfile, err = os.Create(fileName)
    if err != nil {
        fmt.Println("Error opening file for write ", fileName, err)
    }

    // processArray contains all the new metrics found that need to be processed (sent to collection ingest)
    batchSize := 10000
    processMap := make(map[string]ServerMetric)
    var maxBytesToLoad int64 = 5000
    processCount := 1

    // dataDir is where all of the collectd server metrics are written
    dataDir := "/var/lib/collectd"
    now := time.Now()
    filepath.Walk(dataDir, func(path string, info os.FileInfo, err error) error {
        if (!info.IsDir()) {
            fmt.Println("File Process Count:", processCount)
            processCount += 1

            hoursSinceUpdate := now.Sub(info.ModTime()).Hours()

            if (hoursSinceUpdate < timeThreshold) {
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
                    if (bytesDifference > maxBytesToLoad) {
                        bytesDifference = maxBytesToLoad
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
                            if len(time) >= minEpochTimeLength {
                                metric := ServerMetric{Host: server, Collection: collection, Metric: metric, Time: time, Value: value}
                                processMap[string(len(processMap))] = metric

                                // batch results that we'll push to data ingest
                                if (len(processMap) >= batchSize) {
                                    fmt.Println("processCount, batchSize ", len(processMap), batchSize)
                                    sendData(processMap)
                                    processMap = nil
                                    processMap = make(map[string]ServerMetric)
                                }
                            }
                        }
                    }
                    lfile.Close()
                }

                // write the meta data about how many bytes were in this file.  These bytes are ingored on the next data run
                newLine := fmt.Sprint(key, "~", info.Size(), "\n")
                pfile.WriteString(newLine)
            }
        }
        //exit directory walk
        return nil
    })
    if (len(processMap) > 0) {
        sendData(processMap)
        processMap = nil
        processMap = make(map[string]ServerMetric)
    }
    pfile.Sync()
    pfile.Close()
    fmt.Println("finished", time.Now())
}

func sendData(mapIn map[string]ServerMetric) {
    fmt.Println("send data", len(mapIn))
    url := "http://69.252.123.113:8085/ingest"

    fmt.Println(time.Now(), "format json")
    // build json string
    //jsonstr:= "["
    //i := 0
    //for _, obj := range mapIn {
    //    line, err := json.Marshal(obj)
    //    if (err != nil) {
    //        fmt.Println("error in marchal json ", err)
    //    } else {
    //        if (i > 0) {
    //            jsonstr += ","
    //        }
    //        jsonstr += string(line)
    //        i +=1
    //    }
    //}
    //jsonstr += "]"
    jsonstr, err := json.Marshal(mapIn)

    fmt.Println(time.Now(), "do POST")
    var jsonBytes = []byte(jsonstr)
    req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonBytes))
    req.Header.Set("Content-Type", "application/json")
    req.Header.Set("Accept", "application/json")

    client := &http.Client{}
    resp, err := client.Do(req)
    if err != nil {
        panic(err)
    }
    defer resp.Body.Close()

    fmt.Println("response Status:", resp.Status)
    fmt.Println("response Headers:", resp.Header)
    //body, _ := ioutil.ReadAll(resp.Body)

    broker := kafka.NewBrokerPublisher("localhost:9092", "mytesttopic", 0)
    broker.Publish(kafka.NewMessage([]byte("tesing 1 2 3")))
}
