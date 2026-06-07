package common

import (
	"context"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Connections aggregates connections to MongoDB, Redis, and RabbitMQ
type Connections struct {
	MongoClient *mongo.Client
	RedisClient *redis.Client
	RabbitMQ    *RabbitMQConn
}

// RabbitMQConn wraps the RabbitMQ connection and channel
type RabbitMQConn struct {
	Conn    *amqp.Connection
	Channel *amqp.Channel
}

// ConnectAll initializes connections to MongoDB, Redis, and RabbitMQ
func ConnectAll(cfg Config) (*Connections, error) {
	// 1. Connect to MongoDB
	mongoCtx, mongoCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer mongoCancel()

	mongoClientOpts := options.Client().ApplyURI(cfg.MongoURI)
	mongoClient, err := mongo.Connect(mongoCtx, mongoClientOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to mongodb: %w", err)
	}
	if err := mongoClient.Ping(mongoCtx, nil); err != nil {
		return nil, fmt.Errorf("failed to ping mongodb: %w", err)
	}
	log.Println("common: Successfully connected to MongoDB")

	// 2. Connect to Redis
	redisClient := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	})
	redisCtx, redisCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer redisCancel()

	if _, err := redisClient.Ping(redisCtx).Result(); err != nil {
		mongoClient.Disconnect(context.Background())
		return nil, fmt.Errorf("failed to connect to redis: %w", err)
	}
	log.Println("common: Successfully connected to Redis")

	// 3. Connect to RabbitMQ
	rabbitConn, err := amqp.Dial(cfg.RabbitMQURI)
	if err != nil {
		mongoClient.Disconnect(context.Background())
		redisClient.Close()
		return nil, fmt.Errorf("failed to connect to rabbitmq: %w", err)
	}

	rabbitChan, err := rabbitConn.Channel()
	if err != nil {
		mongoClient.Disconnect(context.Background())
		redisClient.Close()
		rabbitConn.Close()
		return nil, fmt.Errorf("failed to open rabbitmq channel: %w", err)
	}
	log.Println("common: Successfully connected to RabbitMQ")

	return &Connections{
		MongoClient: mongoClient,
		RedisClient: redisClient,
		RabbitMQ: &RabbitMQConn{
			Conn:    rabbitConn,
			Channel: rabbitChan,
		},
	}, nil
}

// Close gracefully closes all active connections
func (c *Connections) Close() {
	if c.MongoClient != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := c.MongoClient.Disconnect(ctx); err != nil {
			log.Printf("common: Error closing MongoDB connection: %v\n", err)
		} else {
			log.Println("common: Closed MongoDB connection cleanly")
		}
		cancel()
	}
	if c.RedisClient != nil {
		if err := c.RedisClient.Close(); err != nil {
			log.Printf("common: Error closing Redis connection: %v\n", err)
		} else {
			log.Println("common: Closed Redis connection cleanly")
		}
	}
	if c.RabbitMQ != nil {
		if c.RabbitMQ.Channel != nil {
			c.RabbitMQ.Channel.Close()
		}
		if c.RabbitMQ.Conn != nil {
			c.RabbitMQ.Conn.Close()
		}
		log.Println("common: Closed RabbitMQ connection cleanly")
	}
}
