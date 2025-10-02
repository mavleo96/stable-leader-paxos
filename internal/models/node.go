package models

// Node represents a node in the distributed system
type Node struct {
	ID      string `yaml:"id"`
	Address string `yaml:"address"`
}
