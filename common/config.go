package common

import (
	"log"
	"os"
	"path/filepath"

	"github.com/spf13/viper"
)

type Config struct {
	MongoURI      string `mapstructure:"MONGO_URI"`
	RedisAddr     string `mapstructure:"REDIS_ADDR"`
	RedisPassword string `mapstructure:"REDIS_PASSWORD"`
	RedisDB       int    `mapstructure:"REDIS_DB"`
	RabbitMQURI   string `mapstructure:"RABBITMQ_URI"`
}

var AppConfig Config

func InitConfig() error {
	viper.SetConfigType("env")

	// Walk up directories to find the .env file
	dir, err := os.Getwd()
	if err == nil {
		for {
			envPath := filepath.Join(dir, ".env")
			if _, err := os.Stat(envPath); err == nil {
				viper.SetConfigFile(envPath)
				break
			}
			parent := filepath.Dir(dir)
			if parent == dir {
				break
			}
			dir = parent
		}
	}

	// Read environment variables from the system to override file settings
	viper.AutomaticEnv()

	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return err
		}
		log.Printf("common/config: No .env file found. Falling back to system environment variables.")
	}

	if err := viper.Unmarshal(&AppConfig); err != nil {
		return err
	}
	return nil
}

func init() {
	_ = InitConfig()
}
