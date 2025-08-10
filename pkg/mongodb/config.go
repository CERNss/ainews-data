package mongodb

import (
	"gopkg.in/yaml.v3"
	"os"
)

type MongoConfig struct {
	Host       string `yaml:"host"`
	DBName     string `yaml:"dbname"`
	Username   string `yaml:"username"`
	Password   string `yaml:"password"`
	AuthSource string `yaml:"authSource"`
}

type Config struct {
	Mongo MongoConfig `yaml:"mongo"`
}

func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}
