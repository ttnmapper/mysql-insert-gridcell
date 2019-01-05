package main

import (
	"encoding/json"
	_ "github.com/go-sql-driver/mysql"
	"github.com/j4/gosm"
	"github.com/jmoiron/sqlx"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/streadway/amqp"
	"log"
	"net/http"
	"os"
	"time"
	"ttnmapper-mysql-insert-gridcell/types"
)

var messageChannel = make(chan types.TtnMapperUplinkMessage)

type Configuration struct {
	AmqpHost     string
	AmqpPort     string
	AmqpUser     string
	AmqpPassword string

	MysqlHost     string
	MysqlPort     string
	MysqlUser     string
	MysqlPassword string
	MysqlDatabase string

	PromethuesPort string
}

var myConfiguration = Configuration{
	AmqpHost:     "localhost",
	AmqpPort:     "5672",
	AmqpUser:     "user",
	AmqpPassword: "password",

	MysqlHost:     "localhost",
	MysqlPort:     "3306",
	MysqlUser:     "user",
	MysqlPassword: "password",
	MysqlDatabase: "database",

	PromethuesPort: "2114",
}

var (
	dbSelects = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ttnmapper_mysql_selects_gridcell_count",
		Help: "The total number of select  queries",
	})

	dbInserts = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ttnmapper_mysql_inserts_gridcell_count",
		Help: "The total number of packets inserted into the aggregate table",
	})

	dbUpdates = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ttnmapper_mysql_updates_gridcell_count",
		Help: "The total number of update",
	})

	gatewayDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "ttnmapper_gridcell_gateway_duration",
		Help:    "How long the processing of a gateway takes",
		Buckets: []float64{0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1, 1.5, 2, 5, 10, 100, 1000, 10000},
	})

	selectDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "ttnmapper_gridcell_select_duration",
		Help:    "How long selecting of previous data takes",
		Buckets: []float64{0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1, 1.5, 2, 5, 10, 100, 1000, 10000},
	})

	insertDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "ttnmapper_gridcell_insert_duration",
		Help:    "How long inserting new data takes",
		Buckets: []float64{0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1, 1.5, 2, 5, 10, 100, 1000, 10000},
	})

	updateDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "ttnmapper_gridcell_update_duration",
		Help:    "How long updating an entry takes",
		Buckets: []float64{0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1, 1.5, 2, 5, 10, 100, 1000, 10000},
	})
)

func main() {

	file, err := os.Open("conf.json")
	if err != nil {
		log.Print(err.Error())
	}
	decoder := json.NewDecoder(file)
	err = decoder.Decode(&myConfiguration)
	if err != nil {
		log.Print(err.Error())
	}
	err = file.Close()
	if err != nil {
		log.Print(err.Error())
	}
	log.Printf("Using configuration: %+v", myConfiguration) // output: [UserA, UserB]

	http.Handle("/metrics", promhttp.Handler())
	go func() {
		err := http.ListenAndServe("127.0.0.1:"+myConfiguration.PromethuesPort, nil)
		if err != nil {
			log.Print(err.Error())
		}
	}()

	// Start hread to handle MySQL inserts
	go insertToMysql()

	// Start amqp listener on this thread - blocking function
	subscribeToRabbit()
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func subscribeToRabbit() {
	conn, err := amqp.Dial("amqp://" + myConfiguration.AmqpUser + ":" + myConfiguration.AmqpPassword + "@" + myConfiguration.AmqpHost + ":" + myConfiguration.AmqpPort + "/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	err = ch.ExchangeDeclare(
		"new_packets", // name
		"fanout",      // type
		true,          // durable
		false,         // auto-deleted
		false,         // internal
		false,         // no-wait
		nil,           // arguments
	)
	failOnError(err, "Failed to declare an exchange")

	q, err := ch.QueueDeclare(
		"mysql_insert_gridcell", // name
		false,                   // durable
		false,                   // delete when usused
		false,                   // exclusive
		false,                   // no-wait
		nil,                     // arguments
	)
	failOnError(err, "Failed to declare a queue")

	err = ch.QueueBind(
		q.Name,        // queue name
		"",            // routing key
		"new_packets", // exchange
		false,
		nil)
	failOnError(err, "Failed to bind a queue")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			//log.Printf(" [a] %s", d.Body)
			var packet types.TtnMapperUplinkMessage
			if err := json.Unmarshal(d.Body, &packet); err != nil {
				log.Print(" [a] " + err.Error())
				continue
			}
			log.Print(" [a] Packet received")
			messageChannel <- packet
		}
	}()

	log.Printf(" [a] Waiting for packets. To exit press CTRL+C")
	<-forever

}

func insertToMysql() {

	db, err := sqlx.Open("mysql", myConfiguration.MysqlUser+":"+myConfiguration.MysqlPassword+"@tcp("+myConfiguration.MysqlHost+":"+myConfiguration.MysqlPort+")/"+myConfiguration.MysqlDatabase+"?parseTime=true")
	if err != nil {
		panic(err.Error())
	}
	err = db.Ping()
	if err != nil {
		panic(err.Error())
	}
	defer db.Close()

	stmtSelect, err := db.PrepareNamed("SELECT * FROM agg_zoom_19 WHERE gtw_id = :gtw_id AND x = :x AND y = :y")
	if err != nil {
		panic(err.Error())
	}
	defer stmtSelect.Close()

	stmtInsert, err := db.PrepareNamed("INSERT INTO agg_zoom_19 " +
		"(gtw_id, first_sample, " +
		"x, y, " +
		"bucket_high, bucket_100, bucket_105, bucket_110, " +
		"bucket_115, bucket_120, bucket_125, bucket_130, " +
		"bucket_135, bucket_140, bucket_145, bucket_low, bucket_no_signal) " +
		"VALUES " +
		"(:gtw_id, :first_sample, " +
		":x, :y, " +
		":bucket_high, :bucket_100, :bucket_105, :bucket_110, " +
		":bucket_115, :bucket_120, :bucket_125, :bucket_130, " +
		":bucket_135, :bucket_140, :bucket_145, :bucket_low, :bucket_no_signal)")
	if err != nil {
		panic(err.Error())
	}
	defer stmtInsert.Close()

	stmtUpdate, err := db.PrepareNamed("UPDATE `agg_zoom_19` SET " +
		"last_sample = :last_sample, " +
		"bucket_high = :bucket_high, bucket_100 = :bucket_100, bucket_105 = :bucket_105, " +
		"bucket_110 = :bucket_110, bucket_115 = :bucket_115, bucket_120 = :bucket_120, " +
		"bucket_125 = :bucket_125, bucket_130 = :bucket_130, bucket_135 = :bucket_135, " +
		"bucket_140 = :bucket_140, bucket_145 = :bucket_145, bucket_low = :bucket_low, " +
		"bucket_no_signal = :bucket_no_signal " +
		"WHERE gtw_id = :gtw_id AND x = :x AND y = :y")
	if err != nil {
		panic(err.Error())
	}
	defer stmtUpdate.Close()

	for {
		message := <-messageChannel
		log.Printf(" [m] Processing packet")

		if message.TtnMExperiment != "" {
			log.Print("  [m] Experiment, not adding to aggregated data")
			continue
		}

		tile := gosm.NewTileWithLatLong(message.TtnMLatitude, message.TtnMLongitude, 19)

		for _, gateway := range message.Metadata.Gateways {

			gatewayStart := time.Now()

			m := map[string]interface{}{"gtw_id": gateway.GtwID, "x": tile.X, "y": tile.Y}
			log.Printf("%v", m)
			var entry = types.MysqlAggGridcell{}

			selectStart := time.Now()
			result := stmtSelect.QueryRow(m)
			err = result.StructScan(&entry)
			if err != nil {
				log.Print(err.Error())
			}
			dbSelects.Inc()
			selectElapsed := time.Since(selectStart)
			selectDuration.Observe(float64(selectElapsed) / 1000.0 / 1000.0)

			log.Printf("%v", entry)
			updateEntry(&entry, message, gateway)

			updateStart := time.Now()
			updateResult, err := stmtUpdate.Exec(entry)
			if err != nil {
				log.Print(err.Error())
			}
			rowsAffected, err := updateResult.RowsAffected()
			if err != nil {
				log.Print(err.Error())
			}
			updateElapsed := time.Since(updateStart)
			updateDuration.Observe(float64(updateElapsed) / 1000.0 / 1000.0)

			if rowsAffected > 0 {
				log.Printf("  [m] Updated %d rows", rowsAffected)
				dbUpdates.Inc()
			} else {

				insertStart := time.Now()

				insertResult, err := stmtInsert.Exec(entry)
				if err != nil {
					log.Print(err.Error())
				} else {
					lastId, err := insertResult.LastInsertId()
					if err != nil {
						log.Print(err.Error())
					}

					rowsAffected, err := insertResult.RowsAffected()
					if err != nil {
						log.Print(err.Error())
					}

					log.Printf("  [m] Inserted entry id=%d (affected %d rows)", lastId, rowsAffected)
					dbInserts.Inc()
				}

				insertElapsed := time.Since(insertStart)
				insertDuration.Observe(float64(insertElapsed) / 1000.0 / 1000.0) //nanoseconds to milliseconds

			}

			gatewayElapsed := time.Since(gatewayStart)
			gatewayDuration.Observe(float64(gatewayElapsed.Nanoseconds()) / 1000.0 / 1000.0) //nanoseconds to milliseconds
		}

	}
}

func updateEntry(entry *types.MysqlAggGridcell, message types.TtnMapperUplinkMessage, gateway types.GatewayMetadata) {
	entry.GtwId = gateway.GtwID

	messageTime := types.NullTime{message.Metadata.Time.GetTime(), true}
	if !entry.FirstSample.Valid {
		entry.FirstSample = messageTime
	}
	if messageTime.Time.Before(entry.FirstSample.Time) {
		entry.FirstSample = messageTime
	}
	if messageTime.Time.After(entry.LastSample.Time) {
		entry.LastSample = messageTime
	}

	tile := gosm.NewTileWithLatLong(message.TtnMLatitude, message.TtnMLongitude, 19)
	entry.X = tile.X
	entry.Y = tile.Y

	signal := gateway.RSSI
	if gateway.SNR < 0 {
		signal += gateway.SNR
	}

	if signal > -97.5 {
		entry.BucketHigh++
	} else if signal > -102.5 {
		entry.Bucket100++
	} else if signal > -107.5 {
		entry.Bucket105++
	} else if signal > -112.5 {
		entry.Bucket110++
	} else if signal > -117.5 {
		entry.Bucket115++
	} else if signal > -122.5 {
		entry.Bucket120++
	} else if signal > -127.5 {
		entry.Bucket125++
	} else if signal > -132.5 {
		entry.Bucket130++
	} else if signal > -137.5 {
		entry.Bucket135++
	} else if signal > -142.5 {
		entry.Bucket140++
	} else if signal > -147.5 {
		entry.Bucket145++
	} else {
		entry.BucketLow++
	}

}

func messageToEntry(message types.TtnMapperUplinkMessage, gateway types.GatewayMetadata) types.MysqlAggGridcell {
	var entry = types.MysqlAggGridcell{}

	//if gateway.Time != types.BuildTime(0) {
	//	entry.Time = gateway.Time
	//} else {
	//	entry.Time = message.Metadata.Time
	//}
	//entry.Time = message.Metadata.Time.GetTime() // Do not trust gateway time - always use server time
	//
	//entry.GtwId = gateway.GtwID
	//
	//entry.Modulation = message.Metadata.Modulation
	//entry.DataRate = message.Metadata.DataRate
	//entry.Bitrate = message.Metadata.Bitrate
	//entry.CodingRate = message.Metadata.CodingRate
	//
	//entry.Frequency = message.Metadata.Frequency
	//entry.RSSI = gateway.RSSI
	//entry.SNR = gateway.SNR
	//
	//entry.Latitude = message.TtnMLatitude
	//entry.Longitude = message.TtnMLongitude
	//entry.Altitude = message.TtnMAltitude

	return entry
}
