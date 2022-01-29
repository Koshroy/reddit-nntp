package config

import (
	"fmt"
	"log"
	"os"

	toml "github.com/pelletier/go-toml/v2"
)

type Config struct {
	ConcurrencyLimit uint
	Listener         string
	Subreddits       []string
}

func ParseFile(path string) (*Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("error opening config file: %w", err)
	}
	defer func() {
		err := f.Close()
		if err != nil {
			log.Println("error closing file:", err)
		}
	}()

	d := toml.NewDecoder(f)
	var config Config
	err = d.Decode(&config)
	if err != nil {
		return nil, fmt.Errorf("error parsing TOML: %w", err)
	}

	return &config, err
}
