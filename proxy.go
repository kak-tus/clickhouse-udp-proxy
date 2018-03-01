package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"reflect"
	"strconv"
	"time"

	"github.com/kshvakov/clickhouse"
)

type reqType struct {
	Query   string        `json:"query"`
	Data    []interface{} `json:"data"`
	Types   []string      `json:"types"`
	Version int           `json:"version"`
}

var db *sql.DB
var logger = log.New(os.Stdout, "", log.LstdFlags)
var errLogger = log.New(os.Stderr, "", log.LstdFlags)

func main() {
	connectDB()

	ch := make(chan reqType)
	go aggregate(ch)

	listen(ch)
}

func connectDB() {
	addr := os.Getenv("CLICKHOUSE_ADDR")

	var err error
	db, err = sql.Open("clickhouse", "tcp://"+addr+"?write_timeout=20")
	if err != nil {
		log.Panicln(err)
	}

	err = db.Ping()
	if err != nil {
		exception, ok := err.(*clickhouse.Exception)
		if ok {
			log.Panicln(fmt.Sprintf("[%d] %s \n%s", exception.Code, exception.Message, exception.StackTrace))
		} else {
			log.Panicln(err)
		}
	}

	return
}

func listen(ch chan reqType) {
	conn, err := net.ListenPacket("udp", ":9001")
	if err != nil {
		logger.Panicln(err)
	}

	buf := make([]byte, 65535)

	for {
		num, _, err := conn.ReadFrom(buf)
		if err != nil {
			errLogger.Println(err)
			continue
		}

		var parsed reqType
		err = json.Unmarshal(buf[0:num], &parsed)
		if err != nil {
			errLogger.Println(err)
			continue
		}

		ch <- parsed
	}
}

func aggregate(ch chan reqType) {
	period, err := strconv.ParseUint(os.Getenv("PROXY_PERIOD"), 10, 64)
	if err != nil {
		errLogger.Println(err)
		period = 5
	}

	batch, err := strconv.ParseUint(os.Getenv("PROXY_BATCH"), 10, 64)
	if err != nil {
		errLogger.Println(err)
		batch = 10000
	}

	parsedVals := make(map[string][]reqType)
	parsedCnts := make(map[string]int)

	start := time.Now()

	for {
		parsed := <-ch

		if parsedVals[parsed.Query] == nil {
			parsedVals[parsed.Query] = make([]reqType, batch)
		}

		parsedCnts[parsed.Query]++
		parsedVals[parsed.Query][parsedCnts[parsed.Query]-1] = parsed

		if parsedCnts[parsed.Query] >= int(batch) {
			_ = send(parsed.Query, parsedVals[parsed.Query][0:parsedCnts[parsed.Query]])
			logger.Println(fmt.Sprintf("Sended %d values for %q", parsedCnts[parsed.Query], parsed.Query))
			parsedCnts[parsed.Query] = 0
		}

		if time.Now().Sub(start).Seconds() >= float64(period) {
			for k, v := range parsedVals {
				if parsedCnts[k] > 0 {
					_ = send(k, v[0:parsedCnts[k]])
					logger.Println(fmt.Sprintf("Sended %d values for %q", parsedCnts[k], k))
					parsedCnts[k] = 0
				}
			}

			start = time.Now()
		}
	}
}

func send(query string, vals []reqType) error {
	tx, err := db.Begin()
	if err != nil {
		errLogger.Println(err)
		return err
	}

	stmt, err := tx.Prepare(query)
	if err != nil {
		errLogger.Println(err)
		return err
	}

	for _, val := range vals {
		var args []interface{}
		argErr := 0

		for i := 0; i < len(val.Data); i++ {
			if val.Types[i] == "int" && reflect.TypeOf(val.Data[i]).Kind() == reflect.String {
				errLogger.Println(fmt.Sprintf("Got type string, but waited int: %q", val))
				argErr = 1
			} else if val.Types[i] == "int" {
				args = append(args, int64(val.Data[i].(float64)))
			} else {
				args = append(args, val.Data[i])
			}
		}

		if argErr == 0 {
			_, err := stmt.Exec(args...)

			if err != nil {
				errLogger.Println(err)
				return err
			}
		}
	}

	err = tx.Commit()
	if err != nil {
		errLogger.Println(err)
		return err
	}

	return nil
}
