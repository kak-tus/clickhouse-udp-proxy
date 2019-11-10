package main

import (
	"net/http"
	"os"
	"os/signal"

	"github.com/kak-tus/clickhouse-udp-proxy/listener"
	"github.com/kak-tus/healthcheck"
	"go.uber.org/zap"
)

func main() {
	logger, err := zap.NewProduction()
	if err != nil {
		panic(err)
	}

	log := logger.Sugar()

	cnf, err := newConfig()
	if err != nil {
		log.Panic(err)
	}

	hlth := healthcheck.NewHandler()
	hlth.Add("/healthcheck", func() (healthcheck.State, string) {
		return healthcheck.StatePassing, "ok"
	})

	lstn, err := listener.NewListener(cnf.Listener, log)
	if err != nil {
		log.Panic(err)
	}

	go func() {
		if err := http.ListenAndServe(cnf.Healthcheck.Listen, hlth); err != nil {
			log.Panic(err)
		}
	}()

	go lstn.Start()

	st := make(chan os.Signal, 1)
	signal.Notify(st, os.Interrupt)

	<-st
	log.Info("Stop")

	lstn.Stop()

	_ = log.Sync()
}
