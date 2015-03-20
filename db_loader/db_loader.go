package main

import (
    "fmt"
    "github.com/Shopify/sarama"
    "time"
    "database/sql"
    "github.com/go-sql-driver/mysql"
    "encoding/json"
    "strconv"
    "strings"
    "github.com/couchbaselabs/go-couchbase"
    "os"
    "runtime"
    "math/rand"
    "regexp"
)

// ServerMetric stores 1 time series metric line from collectd.
type ServerMetric struct {
    Host string
    Collection string
    Metric string
    Time string
    Value string
}

const MILLIS_IN_AN_HOUR int64 = 3600000
const CLIENT_ID string = "plexus-collectd"
const KAFKA_TOPIC string = "sde-collectd"
const LOG_FILE_NAME = "/var/log/plexus_data_load_processing.log"
const DB_HOST = "10.22.236.82"
const DB_NAME = "icarus"
const DB_PORT = "3306"
const DB_U = "caliper"
const DB_P = "c0mcast!"
var KAFKA_BROKERS = []string{"172.30.18.139:9092","172.30.18.140:9092"}
var logFile *os.File

func main() {
    Initialize()
    RunProcessing(true)
}

func Initialize() {
    logFile,_ = os.Create(LOG_FILE_NAME)
    runtime.GOMAXPROCS(2)

    // force detection/ import of mysql driver
    _ = mysql.NullTime{}
}

// Run processing will read new messages from kafka and load the data into our database(s)
// This function runs continuously if continuous = true
func RunProcessing(continuous bool) int {
    rowsProcessed := 0
    // setup the mysql db connection pool
    connString := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", DB_U, DB_P, DB_HOST, DB_PORT, DB_NAME)
    fmt.Println("conn string ", connString)
    //db,_ :=sql.Open("mysql", "vipersde:c0mcast!@tcp(10.22.237.123:3306)/collections")
    db,_ :=sql.Open("mysql", connString)
    defer db.Close()

    // run deletes in separate thread
    go mysql_delete_old_data(db, continuous)

    // get host names - for data simulation on all production hosts
    hostNames := getHostNames(db)

    fmt.Println("get Kafka client")

    // set up the sarama client
    clientConfig := sarama.NewClientConfig()
    client,err := sarama.NewClient(CLIENT_ID, KAFKA_BROKERS, clientConfig)
    if err != nil {
        fmt.Println("error creating Kafka client", err)
    }
    defer client.Close()

    // set up the sarama consumer (a client is either a publisher or a consumer)
    consumerConfig := sarama.NewConsumerConfig()
    consumer,err := sarama.NewConsumer(client, consumerConfig)
    if err != nil {
        fmt.Println("error creating Kafka consumer", err)
    }

    fmt.Println("get partitions")

    // get the available partitions for this topic.  we'll be looking for messages on each of these partitions
    partitionValues,err := client.Partitions(KAFKA_TOPIC)
    if err != nil {
        fmt.Println("error getting Kafka partitions", err)
    }
    partitions := make([]*sarama.PartitionConsumer, len(partitionValues))
    partitionConsumerConfig := sarama.NewPartitionConsumerConfig()
    for i := 0; i < len(partitionValues); i++ {
        partitionValue := partitionValues[i]
        partition,_ := consumer.ConsumePartition(KAFKA_TOPIC, partitionValue, partitionConsumerConfig)
        defer partition.Close()
        partitions[i] = partition
        fmt.Println("found partition", partition)
    }

    for {
        fmt.Println(time.Now(), "START loop")
        // if this function was called in a non continuous request, we want to wait a few seconds to allow some messages to come in
        if !continuous {
            fmt.Println("wait a few seconds for new messages")
            time.Sleep(6 * time.Second)
        }

        // loop through all available partitions looking for messages on each partition
        for j := 0; j < len(partitions); j++ {
            partition := partitions[j]
            select {
            case msg := <-partition.Messages():
                fmt.Println(time.Now(), "kafka messages found")

                jsonObj := make(map[string][]ServerMetric)
                json.Unmarshal(msg.Value, &jsonObj)
                rowsProcessed += mysql_load_data(jsonObj, db, hostNames)
                couchbase_db_load(jsonObj)
            default:
                //fmt.Println(time.Now(), "no read messages")
            }
        }

        if !continuous {
            break
        }
        time.Sleep(1 * time.Second)
        fmt.Println(time.Now(), "FINISH loop")

    }
    fmt.Println(time.Now(), "finished - run deferred close next")
    return rowsProcessed
}

// This function will delete data older than <hoursToDelete> from the mySQL DB table
// This function runs continuously if continuous = true
func mysql_delete_old_data(db *sql.DB, continuous bool) {
    fmt.Println(time.Now(), "Run MySQL Delete")
    var hoursToDelete int64 = 1
    for {
        var affectedRows int64 = 1
        timeCheck := (time.Now().UnixNano() / 1000000 ) - (hoursToDelete * MILLIS_IN_AN_HOUR)
        valueArgs := make([]interface{}, 0, 1)
        valueArgs = append(valueArgs, timeCheck)
        // need to put a limit on the delete to prevent going over max transaction limits on cluster
        sql := fmt.Sprintf("DELETE FROM plexus_ndb WHERE time < ? LIMIT 10000")

        //loop through small DELETE operations until no rows are affected (workaround for cluster transaction limits)
        for affectedRows > 0 {
            //fmt.Println(time.Now(), "Start DELETE old rows")
            res, err := db.Exec(sql, valueArgs...)
            if err != nil {
                fmt.Println("ERROR on delete", err)
            } else {
                affectedRows, err = res.RowsAffected()
                //fmt.Println("affected", affectedRows)
            }
            //fmt.Println(time.Now(), "End DELETE old rows")
        }

        if !continuous {
            break
        }
        fmt.Println("SLEEP on delete thread")
        time.Sleep(60 * time.Second)

    }

    fmt.Println(time.Now(), "DELETE COMPLETE - mysql")
}

func mysql_load_data(jsonObj map[string][]ServerMetric, db *sql.DB, hostNames []string) int {
    fmt.Println(time.Now(), "run mysql load")
    rowsProcessed := 0

    t := time.Now()
    dateStr := "-" +t.Format("2006-01-02")
    reg1, _ := regexp.Compile(dateStr)

    for _, metricsList := range jsonObj {
        fmt.Println(time.Now(), "data rows to load:", len(metricsList))

        columnCount := 5
        valueStrings := make([]string, 0, len(metricsList))
        valueArgs := make([]interface{}, 0, len(metricsList) * columnCount)

        for i:=0; i<len(metricsList); i++ {
            var metric ServerMetric = metricsList[i]


            timeF,_ := strconv.ParseFloat(metric.Time,64)
            timeI := int(timeF * 1000)
            valueF,_ := strconv.ParseFloat(metric.Value,64)
            valueStrings = append(valueStrings, "(?, ?, ?, ?, ?)")
            valueArgs = append(valueArgs, metric.Host)
            valueArgs = append(valueArgs, metric.Collection)
            valueArgs = append(valueArgs, getMetricShortName(reg1, metric.Metric))
            valueArgs = append(valueArgs, timeI)
            valueArgs = append(valueArgs, valueF)

            rowsProcessed +=1

        }
        stmt := fmt.Sprintf("INSERT INTO plexus_ndb (host_name, collection, metric, time, value) VALUES %s", strings.Join(valueStrings, ","))
        res, err := db.Exec(stmt, valueArgs...)
        if err != nil {
            log(fmt.Sprintf("ERROR in mysql_load_data: %s", err))
            fmt.Println("ERROR in mysql_load_data:", err)
        } else {
            log(fmt.Sprintf("MYSQL_DATA_SIZE:%d", len(metricsList)))
            ra, err:= res.RowsAffected()
            if err != nil {
                fmt.Println("error getting rows affected", err)
            }
            fmt.Println("MYSQL_DATA_SIZE:", len(metricsList), "Rows Affected", ra)
        }
    }

    // this is static data to simulate load across a wide distribution of host names (all hosts from inventory)
    fmt.Println(time.Now(), "begin simulated data inserts")
    commitLength := 1500
    columnCount := 5
    metrics := []string {"cpu-1-one", "cpu-2-two", "cpu-3-three", "cpu-4-four", "cpu-5-five", "cpu-6-six", "cpu-7-seven", "cpu-8-eight", "cpu-9-nine", "cpu-10-ten"}
    valueStrings := make([]string, 0, commitLength)
    valueArgs := make([]interface{}, 0, commitLength * columnCount)
    totalCount := 0
    subCount := 0
    resc := make(chan string)
    hostMap := make(map[string]int)
    for _, metricName := range metrics {
        for _, hostName := range hostNames {
            if (!strings.Contains(hostName, "[")) {
                if len(hostName) < 75 {
                    hostMap[hostName] = 1
                    timeMillis := time.Now().UnixNano()/ 1000000

                    valueStrings = append(valueStrings, "(?, ?, ?, ?, ?)")
                    valueArgs = append(valueArgs, hostName)
                    valueArgs = append(valueArgs, "cpu-1")
                    valueArgs = append(valueArgs, metricName)
                    valueArgs = append(valueArgs, timeMillis)
                    valueArgs = append(valueArgs, rand.Float64()*100)

                    totalCount += 1
                    subCount += 1

                    if subCount == commitLength {
                        stmt := fmt.Sprintf("INSERT INTO plexus_ndb (host_name, collection, metric, time, value) VALUES %s", strings.Join(valueStrings, ","))
                        go mysql_insert_thread(resc, db, stmt, valueArgs)
                        subCount = 0
                        valueStrings = make([]string, 0, commitLength)
                        valueArgs = make([]interface{}, 0, commitLength * columnCount)

                    }
                }
            }
        }
    }

    //wait for all insert threads to complete
    threadCount := (len(hostMap) * len(metrics))/ commitLength
    for i:=0; i<threadCount; i++ {
        select {
        case res := <-resc:
        //do nothing
            _ = res
        }
    }

    fmt.Println(time.Now(), "finished all insert threads - total simulated rows", totalCount)


    return rowsProcessed
}

func mysql_insert_thread(resc chan string, db *sql.DB, stmt string, valueArgs []interface{}) {
    //fmt.Println(time.Now(), "run insert2")
    _, err := db.Exec(stmt, valueArgs...)
    if err != nil {
        log(fmt.Sprintf("ERROR in mysql_load simulated data: %s", err))
    }
    //fmt.Println(time.Now(), "finish insert2")
    resc <- string("done")
}

func couchbase_db_load(jsonObj map[string][]ServerMetric) {
    fmt.Println(time.Now(), "run CouchBase Load")
    couchbaseServer, err := couchbase.Connect("http://10.22.123.143:8091/")
    if err != nil {
        log(fmt.Sprintf("ERROR in get couchbase server: %s", err))
        return
    }

    couchbasePool, err := couchbaseServer.GetPool("default")
    if err != nil {
        log(fmt.Sprintf("ERROR in get couchbase pool: %s", err))
        return
    }

    bucket, err := couchbasePool.GetBucket("raw-plexus-timeseries")
    if err != nil {
        log(fmt.Sprintf("ERROR in couchbase_db_load: %s", err))
        return
    }

    docKey := strconv.FormatInt(time.Now().UnixNano(), 10)
    bucket.Set(docKey, 0, jsonObj)
    bucket.Close()

    metricsList := jsonObj["MetricList"]
    log(fmt.Sprintf("COUCHBASE_DATA_SIZE:%d", len(metricsList)))
}

func getHostNames(db *sql.DB) []string {
    fmt.Println(time.Now(), "setHostNames")
    str := "SELECT DISTINCT host_name FROM inventory"
    stmt,_ := db.Prepare(str)
    rows,_ := stmt.Query()
    hostNames := make([]string, 0)
    for rows.Next() {
        var hostname string
        rows.Scan(&hostname)
        hostNames = append(hostNames, hostname)
    }

    fmt.Println(time.Now(), "finished setHostNames")
    return hostNames
}

func getMetricShortName(reg *regexp.Regexp, metric string) string {
    //remove date from metric name
    //idx := strings.Index(metric, reg)
    idxL := reg.FindStringIndex(metric)
    if len(idxL) > 0 {
        idx := idxL[0]
        metric = metric[0:idx]
    }
    return metric
}

func log(message string) {
    str := fmt.Sprintf("%s - %s\n", time.Now(), message)
    logFile.WriteString(str)
}


