package main

import (
	"github.com/iph0/conf"
	"github.com/iph0/conf/envconf"
	"github.com/iph0/conf/fileconf"
	"github.com/kak-tus/clickhouse-udp-proxy/listener"
)

type config struct {
	Healthcheck healthcheckConfig
	Listener    listener.Config
}

type healthcheckConfig struct {
	Listen string
}

func newConfig() (*config, error) {
	fileLdr := fileconf.NewLoader("etc", "/etc")
	envLdr := envconf.NewLoader()

	configProc := conf.NewProcessor(
		conf.ProcessorConfig{
			Loaders: map[string]conf.Loader{
				"file": fileLdr,
				"env":  envLdr,
			},
		},
	)

	configRaw, err := configProc.Load(
		"file:clickhouse-udp-proxy.yml",
		"env:^PROXY_",
	)

	if err != nil {
		return nil, err
	}

	var cnf config
	if err := conf.Decode(configRaw, &cnf); err != nil {
		return nil, err
	}

	return &cnf, nil
}
