package config

import (
	"os"

	"gopkg.in/yaml.v3"
)

// Config holds the configuration for the distributed system
type Config struct {
	Nodes       map[string]*NodeEntry `yaml:"nodes"`
	Clients     []string              `yaml:"clients"`
	DBDir       string                `yaml:"db_dir"`
	InitBalance int64                 `yaml:"init_balance"`
}

// NodeEntry represents a node entry in the config file
type NodeEntry struct {
	ID      string `yaml:"id"`
	Address string `yaml:"address"`
}

// ParseConfig parses the config file and returns a Config struct
func ParseConfig(cfgPath string) (*Config, error) {
	data, err := os.ReadFile(cfgPath)
	if err != nil {
		return &Config{}, err
	}
	var cfg Config
	err = yaml.Unmarshal(data, &cfg)
	if err != nil {
		return &Config{}, err
	}

	return &cfg, nil
}
