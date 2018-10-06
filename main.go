package main

import (
	"git.aqq.me/go/app/appconf"
	"git.aqq.me/go/app/launcher"
	"github.com/iph0/conf/envconf"
	"github.com/iph0/conf/fileconf"
	"github.com/kak-tus/clickhouse-udp-proxy/listener"
	"github.com/kak-tus/healthcheck"
)

func init() {
	fileLdr := fileconf.NewLoader("etc")
	envLdr := envconf.NewLoader()

	appconf.RegisterLoader("file", fileLdr)
	appconf.RegisterLoader("env", envLdr)

	appconf.Require("file:clickhouse-udp-proxy.yml")
	appconf.Require("env:^PROXY_")
}

func main() {
	launcher.Run(func() error {
		healthcheck.Add("/healthcheck", func() (healthcheck.State, string) {
			return healthcheck.StatePassing, "ok"
		})

		go listener.GetListener().Start()

		return nil
	})
}
