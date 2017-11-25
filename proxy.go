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

	return db
}

func listen(ch chan reqType) {
	l := log.New(os.Stderr, "", 0)

	conn, err := net.ListenPacket("udp", ":9001")
	if err != nil {
		log.Panicln(err)
	}

	buf := make([]byte, 65535)

	for {
		num, _, err := conn.ReadFrom(buf)
		if err != nil {
			l.Println(err)
			continue
		}

		var parsed reqType
		err = json.Unmarshal(buf[0:num], &parsed)
		if err != nil {
			l.Println(err)
			continue
		}

		ch <- parsed
	}
}

func aggregate(db *sql.DB, ch chan reqType) {
	l := log.New(os.Stderr, "", 0)
	parsedVals := make(map[string][]reqType)

	period, err := strconv.ParseUint(os.Getenv("PROXY_PERIOD"), 10, 64)
	if err != nil {
		l.Println(err)
		period = 5
	}

	batch, err := strconv.ParseUint(os.Getenv("PROXY_BATCH"), 10, 64)
	if err != nil {
		l.Println(err)
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
			if len(parsedVals[k]) > 0 {
				_ = send(db, k, v)
				log.Println(fmt.Sprintf("Sended %d values for %q", len(parsedVals[k]), k))
				parsedVals[k] = parsedVals[k][:0]
			}
		}
	}
}

func send(db *sql.DB, query string, vals []reqType) error {
	l := log.New(os.Stderr, "", 0)

	tx, err := db.Begin()
	if err != nil {
		l.Println(err)
		return err
	}

	stmt, err := tx.Prepare(query)
	if err != nil {
		l.Println(err)
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
			l.Println(err)
			return err
		}
	}

	err = tx.Commit()
	if err != nil {
		l.Println(err)
		return err
	}

	return nil
}
