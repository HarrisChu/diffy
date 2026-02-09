package pkg

import (
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Environments map[string]any `yaml:"env"`
	Statements   []string       `yaml:"statements"`
	Jobs         []*Job         `yaml:"jobs"`
}

type Job struct {
	Name  string `yaml:"name"`
	Shell string `yaml:"shell"`
}

func NewConfigFromBytes(bs []byte) (*Config, error) {
	cfg := &Config{}
	err := yaml.Unmarshal(bs, cfg)
	if err != nil {
		return nil, err
	}
	return cfg, nil
}

func NewConfigFromFile(path string) (*Config, error) {
	bs, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return NewConfigFromBytes(bs)
}
