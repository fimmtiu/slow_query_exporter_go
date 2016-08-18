package main

import (
	"flag"
	"github.com/BurntSushi/toml"
	"log"
)

type Config struct {
	LogPath     string // The path to the slow query log
	GraylogHost string // The host running Graylog
	GraylogPort int    // The port to send UDP GELF packets to
	// TODO: Add the other three Graylog config variables.
}

var defaultConfig Config = Config{
	"/var/lib/mysqllogs/mysql-slow.log", // LogPath
	"localhost",                         // GraylogHost
	12201,                               // GraylogPort
}

func GetConfig() Config {
	configFilePath := "slow_query_exporter.conf"

	// Parse the command-line arguments into a Config object.
	var commandLineConf Config
	flag.StringVar(&configFilePath, "c", "slow_query_exporter.conf", "Path to the config file")
	flag.Parse()

	if flag.Arg(0) != "" {
		commandLineConf.LogPath = flag.Arg(0)
	} else {
		commandLineConf.LogPath = defaultConfig.LogPath
	}

	// Read the config file into a separate Config object.
	var conf Config
	err := readConfigFile(configFilePath, &conf)
	if err != nil {
		log.Fatalf("Can't read config file '%s': %s\n", configFilePath, err)
	}

	// Override the config file with the command line values if they differ from the default.
	if commandLineConf.LogPath != defaultConfig.LogPath {
		conf.LogPath = commandLineConf.LogPath
	}

	return conf
}

func readConfigFile(path string, conf *Config) error {
	if _, err := toml.DecodeFile(path, &conf); err != nil {
		return err
	}
	return nil
}
