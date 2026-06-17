package common

import (
	"log/slog"
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
	QueueName     string `mapstructure:"QUEUE_NAME"`
	DropNextUrlRate     float32 `mapstructure:"DROP_NEXT_URL_RATE"`
	TimeDelay	   int 			`mapstructure:"TIME_DELAY"`
	JobTTL        int    `mapstructure:"TTL_JOB"`
	DefaultPolitenessDelay int `mapstructure:"DEFAULT_POLITENESS_DELAY"`
}

var AppConfig Config

func InitConfig() error {
	viper.SetDefault("MONGO_URI", "mongodb://admin:password@localhost:27017")
	viper.SetDefault("REDIS_ADDR", "localhost:6379")
	viper.SetDefault("REDIS_PASSWORD", "")
	viper.SetDefault("REDIS_DB", 0)
	viper.SetDefault("RABBITMQ_URI", "amqp://guest:guest@localhost:5672/")
	viper.SetDefault("QUEUE_NAME", "")
	viper.SetDefault("DROP_NEXT_URL_RATE",0)
	viper.SetDefault("TIME_DELAY",3000)
	viper.SetDefault("TTL_JOB", 12)
	viper.SetDefault("DEFAULT_POLITENESS_DELAY", 3000)
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
		slog.Info("No .env file found. Falling back to system environment variables.")
	}

	if err := viper.Unmarshal(&AppConfig); err != nil {
		return err
	}

	slog.Info("Configuration loaded", "config", AppConfig)
	return nil
}

func init() {
	_ = InitConfig()
}
