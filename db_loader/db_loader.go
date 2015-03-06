package main

import (
    "fmt"
    "github.com/Shopify/sarama"
    "time"
    "database/sql"
_   "github.com/go-sql-driver/mysql"
    "encoding/json"
    "strconv"
    "strings"
)

const MILLIS_IN_AN_HOUR int64 = 3600000
const CLIENT_ID string = "plexus-collectd"
const KAFKA_TOPIC string = "sde-collectd"
var KAFKA_BROKERS = []string{"172.30.18.139:9092","172.30.18.140:9092"}

func main() {
    RunProcessing(false)
}

// Run processing will read new messages from kafka and load the data into our database(s)
// This function runs continuously if continuous = true
func RunProcessing(continuous bool) int {
    rowsProcessed := 0
    // setup the mysql db connection pool
    db,_ :=sql.Open("mysql", "vipersde:c0mcast!@tcp(10.22.237.123:3306)/collections")
    defer db.Close()

    // run deletes in separate thread
    go mysql_delete_old_data(db, continuous)

    // set up the sarama client
    clientConfig := sarama.NewClientConfig()
    client,_ := sarama.NewClient(CLIENT_ID, KAFKA_BROKERS, clientConfig)
    defer client.Close()

    // set up the sarama consumer (a client is either a publisher or a consumer)
    consumerConfig := sarama.NewConsumerConfig()
    consumer,_ := sarama.NewConsumer(client, consumerConfig)

    // get the available partitions for this topic.  we'll be looking for messages on each of these partitions
    partitionValues,_ := client.Partitions(KAFKA_TOPIC)
    partitions := make([]*sarama.PartitionConsumer, len(partitionValues))
    partitionConsumerConfig := sarama.NewPartitionConsumerConfig()
    for i := 0; i < len(partitionValues); i++ {
        partitionValue := partitionValues[i]
        partition,_ := consumer.ConsumePartition(KAFKA_TOPIC, partitionValue, partitionConsumerConfig)
        defer partition.Close()
        partitions[i] = partition
    }

    for {

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
                fmt.Println(time.Now(), "message found length", len(string(msg.Value)))
                rowsProcessed += mysql_load_data(msg.Value, db)
            default:
                //fmt.Println(time.Now(), "no read messages")
            }
        }

        if !continuous {
            break
        }
        time.Sleep(1 * time.Second)

    }
    fmt.Println(time.Now(), "finished - run deferred close next")
    return rowsProcessed
}

func mysql_load_data(dataIn []byte, db *sql.DB) int {
    fmt.Println(time.Now(), "load data")
    rowsProcessed := 0

    obj := make(map[string][]map[string]string)
    json.Unmarshal(dataIn, &obj)
    for _, metricsList := range obj {
        fmt.Println(time.Now(), "data rows to load:", len(metricsList))

        columnCount := 5
        valueStrings := make([]string, 0, len(metricsList))
        valueArgs := make([]interface{}, 0, len(metricsList) * columnCount)

        for i:=0; i<len(metricsList); i++ {
            metricMap := metricsList[i]

            timeF,_ := strconv.ParseFloat(metricMap["Time"],64)
            timeI := int(timeF * 1000)
            valueF,_ := strconv.ParseFloat(metricMap["Value"],64)
            valueStrings = append(valueStrings, "(?, ?, ?, ?, ?)")
            valueArgs = append(valueArgs, metricMap["Host"])
            valueArgs = append(valueArgs, metricMap["Collection"])
            valueArgs = append(valueArgs, metricMap["Metric"])
            valueArgs = append(valueArgs, timeI)
            valueArgs = append(valueArgs, valueF)

            rowsProcessed +=1
        }
        stmt := fmt.Sprintf("INSERT INTO plexus_data (host_name, collection, metric, time, value) VALUES %s", strings.Join(valueStrings, ","))
        _, err := db.Exec(stmt, valueArgs...)
        if err != nil {
            fmt.Println("error on insert", err)
        }
    }


    fmt.Println(time.Now(), "mysql data load complete")
    return rowsProcessed
}

// This function will delete data older than <hoursToDelete> from the mySQL DB table
// This function runs continuously if continuous = true
func mysql_delete_old_data(db *sql.DB, continuous bool) {
    fmt.Println(time.Now(), "Run MySQL Delete")
    var hoursToDelete int64 = 6
    for {
        timeCheck := (time.Now().UnixNano() / int64(time.Millisecond)) - (hoursToDelete * MILLIS_IN_AN_HOUR)
        valueArgs := make([]interface{}, 0, 1)
        valueArgs = append(valueArgs, timeCheck)
        stmt := fmt.Sprintf("DELETE FROM plexus_data WHERE time < ?")
        _, err := db.Exec(stmt, valueArgs...)
        if err != nil {
            fmt.Println("error on delete", err)
        }

        if !continuous {
            break
        }
        time.Sleep(60 * time.Second)

    }

    fmt.Println(time.Now(), "DELETE COMPLETE - mysql")
}



