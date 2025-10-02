package config

import "github.com/mavleo96/cft-mavleo96/internal/models"

// Config holds the configuration for the distributed system
type Config struct {
	Nodes   []models.Node `yaml:"nodes"`
	Clients []string      `yaml:"clients"`
	DBDir   string        `yaml:"db_dir"`
}
