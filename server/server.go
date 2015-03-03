package main

import (
    "encoding/json"
    "fmt"
    "log"
    "net/http"
    "os"
    "time"
)

type ServerMetric struct {
    Host string
    Collection string
    Metric string
    Time string
    Value string
}

func processRequest(rw http.ResponseWriter, req *http.Request) {
    decoder := json.NewDecoder(req.Body)
    var results map[string]ServerMetric   
    err := decoder.Decode(&results)
    if (err != nil) {
        fmt.Println("error in decode ", err)
    }
    now := time.Now()
    fmt.Println("new request", now)

    dateStr := now.Format("2006-01-02T15:04:05Z07:00")
    fileName := fmt.Sprint("/root/Apps/data/", dateStr, ".csv")
    file,_ := os.Create(fileName)
    
    for i:=0; i<len(results); i+=1 {
        obj := results[string(i)]
        fstr := fmt.Sprint(obj.Host, ",", obj.Collection, ",", obj.Metric, ",", obj.Time, ",", obj.Value, "\n")
        file.WriteString(fstr)
    }
    file.Close()
}

func main() {
    http.HandleFunc("/ingest", processRequest)
    log.Fatal(http.ListenAndServe(":8085", nil))
}
