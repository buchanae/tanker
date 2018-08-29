package main

import (
  "github.com/buchanae/tanker/storage"
)

func DefaultConfig() Config {
	return Config{
		BaseURL: "swift://buchanan/tanker/",
		DataDir: ".tanker/data",
		Logging: LoggingConfig{
			Path: ".tanker/logs",
		},
    Storage: storage.DefaultConfig(),
	}
}

type Config struct {
	BaseURL string
	DataDir string
	Logging LoggingConfig
  Storage storage.Config
}

type LoggingConfig struct {
	Path string
}
