package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
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

func main() {
	db := connectDB()

	ch := make(chan reqType)
	go aggregate(db, ch)

	listen(ch)
}

func connectDB() *sql.DB {
	addr := os.Getenv("CLICKHOUSE_ADDR")
	db, err := sql.Open("clickhouse", "tcp://"+addr+"?write_timeout=20")
	if err != nil {
		panic(err)
	}

	err = db.Ping()
	if err != nil {
		exception, ok := err.(*clickhouse.Exception)
		if ok {
			panic(fmt.Sprintf("[%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace))
		} else {
			panic(fmt.Sprintln(err))
		}
	}

	return db
}

func listen(ch chan reqType) {
	conn, err := net.ListenPacket("udp", ":9001")
	if err != nil {
		panic(err)
	}

	buf := make([]byte, 65535)

	for {
		num, _, err := conn.ReadFrom(buf)
		if err != nil {
			log.Println(err)
			continue
		}

		var parsed reqType
		err = json.Unmarshal(buf[0:num], &parsed)
		if err != nil {
			log.Println(err)
			continue
		}

		ch <- parsed
	}
}

func aggregate(db *sql.DB, ch chan reqType) {
	parsedVals := make(map[string][]reqType)

	period, err := strconv.ParseUint(os.Getenv("PROXY_PERIOD"), 10, 64)
	if err != nil {
		log.Println(err)
		period = 5
	}

	batch, err := strconv.ParseUint(os.Getenv("PROXY_BATCH"), 10, 64)
	if err != nil {
		log.Println(err)
		batch = 10000
	}

	for {
		cnt := batch
		start := time.Now()

		for cnt > 0 && time.Now().Sub(start).Seconds() < float64(period) {
			parsed := <-ch

			parsedVals[parsed.Query] = append(parsedVals[parsed.Query], parsed)
			cnt--
		}

		for k, v := range parsedVals {
			// TODO retry
			_ = send(db, k, v)
			parsedVals[k] = parsedVals[k][:0]
		}
	}
}

func send(db *sql.DB, query string, vals []reqType) error {
	tx, err := db.Begin()
	if err != nil {
		log.Println(err)
		return err
	}

	stmt, err := tx.Prepare(query)
	if err != nil {
		log.Println(err)
		return err
	}

	for _, val := range vals {
		var args []interface{}

		for i := 0; i < len(val.Data); i++ {
			if val.Types[i] == "int" {
				args = append(args, int64(val.Data[i].(float64)))
			} else {
				args = append(args, val.Data[i])
			}
		}

		_, err := stmt.Exec(args...)

		if err != nil {
			log.Println(err)
			return err
		}
	}

	err = tx.Commit()
	if err != nil {
		log.Println(err)
		return err
	}

	return nil
}
