package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"

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
		}

		var parsed reqType
		err = json.Unmarshal(buf[0:num], &parsed)
		if err != nil {
			log.Println(err)
		}

		ch <- parsed
	}
}

func aggregate(db *sql.DB, ch chan reqType) {
	cnt := 100
	parsedVals := make(map[string][]reqType)

	for {

		// TODO send by time
		for cnt > 0 {
			parsed := <-ch

			parsedVals[parsed.Query] = append(parsedVals[parsed.Query], parsed)
			cnt--
		}

		cnt = 100

		for k, v := range parsedVals {
			// TODO retry
			_ = send(db, k, v)
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
