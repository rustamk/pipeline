package main

import (
	"errors"
	"flag"
	"fmt"
	"os"

	"github.comcast.com/gomirror/toml"
)

var _ = fmt.Println
var filename string

// Set flag indicating filename to use for configuration
func init() {
	flag.StringVar(&filename, "config", "", "Path to the quixote configuration file.")
}

func getConfigFilename() string {
	flag.Set("config", "config.toml")
	return filename
}

func GetConfig() *Config {
	conf := &Config{
		filename: getConfigFilename(),
	}

	var err error
	if conf.filename == "" {
		conf.err = errors.New("configuration filename must be provided.")
		return conf
	}

	// Open config file and read it into a string.
	var f *os.File
	if f, err = os.OpenFile(conf.filename, os.O_RDONLY, 0644); err != nil {
		conf.err = err
		return conf
	}
	defer f.Close()

	// Determine size of file
	var fi os.FileInfo
	if fi, err = f.Stat(); err != nil {
		conf.err = err
		return conf
	}
	size := fi.Size()
	if size == 0 {
		conf.err = errors.New(fmt.Sprintf("Settings file [%s] is zero bytes\n", conf.filename))
		return conf
	}

	// Create a byte array for the file's contents and read the config file into the array.
	b := make([]byte, size)
	_, err = f.Read(b)

	if _, err = toml.Decode(string(b), conf); err != nil {
		conf.err = err
		return conf
	}

	return conf
}

type Config struct {
	filename string
	err      error

	ChannelSize int `toml:"channel_size"`

	Kafka     kafkaConfig
	Decorator decoratorConfig
	QA        qaConfig
}

func (c *Config) Error() error {
	return c.err
}

type kafkaConfig struct {
	ClientId       string `toml:"client_id"`
	GroupId        string `toml:"group_id"`
	RawTopic       string `toml:"raw_topic_name"`
	DecoratedTopic string `toml:"decorated_topic_name"`
	Servers        []string
}

type decoratorConfig struct {
	Hostname   string
	SiteId     int `toml:"site_id"`
	MaxRetries int
	Timeout    int `toml:"timeout_ms"`
}

func (d *decoratorConfig) GetHostString() string {
	return fmt.Sprintf("http://%s/api/site/%d/servers/%%s/dimensions", d.Hostname, d.SiteId)
}

type qaConfig struct {
	Enabled     bool
	ServerCount int `toml:"server_count"`
}
