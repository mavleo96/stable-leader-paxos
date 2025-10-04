package config

import (
	"os"

	"github.com/mavleo96/cft-mavleo96/internal/models"
	"gopkg.in/yaml.v3"
)

// Config holds the configuration for the distributed system
type Config struct {
	Nodes   []models.Node `yaml:"nodes"`
	Clients []string      `yaml:"clients"`
	DBDir   string        `yaml:"db_dir"`
}

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
