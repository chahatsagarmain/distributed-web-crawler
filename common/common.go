package common

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
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
	mu          sync.RWMutex
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
	slog.Info("Successfully connected to MongoDB")

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
	slog.Info("Successfully connected to Redis")

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
	slog.Info("Successfully connected to RabbitMQ")

	return &Connections{
		MongoClient: mongoClient,
		RedisClient: redisClient,
		RabbitMQ: &RabbitMQConn{
			Conn:    rabbitConn,
			Channel: rabbitChan,
		},
	}, nil
}

// GetRabbitMQ returns the active RabbitMQ connection and channel thread-safely
func (c *Connections) GetRabbitMQ() *RabbitMQConn {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.RabbitMQ
}

// ReconnectRabbitMQ reconnects to RabbitMQ thread-safely
func (c *Connections) ReconnectRabbitMQ(uri string) error {
	rabbitConn, err := amqp.Dial(uri)
	if err != nil {
		return fmt.Errorf("failed to dial rabbitmq: %w", err)
	}

	rabbitChan, err := rabbitConn.Channel()
	if err != nil {
		rabbitConn.Close()
		return fmt.Errorf("failed to open rabbitmq channel: %w", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.RabbitMQ != nil {
		if c.RabbitMQ.Channel != nil {
			_ = c.RabbitMQ.Channel.Close()
		}
		if c.RabbitMQ.Conn != nil {
			_ = c.RabbitMQ.Conn.Close()
		}
	}

	c.RabbitMQ = &RabbitMQConn{
		Conn:    rabbitConn,
		Channel: rabbitChan,
	}

	return nil
}

// StartConnectionMonitor monitors the RabbitMQ connection health and reconnects automatically
func (c *Connections) StartConnectionMonitor(uri string, postReconnect func(*amqp.Channel) error) {
	go func() {
		for {
			rmq := c.GetRabbitMQ()
			if rmq == nil || rmq.Conn == nil {
				slog.Warn("RabbitMQ connection is nil, waiting to reconnect...")
			} else {
				closeChan := make(chan *amqp.Error, 1)
				rmq.Conn.NotifyClose(closeChan)

				// Block until connection is closed
				err := <-closeChan
				if err != nil {
					slog.Error("RabbitMQ connection closed with error", "error", err)
				} else {
					slog.Info("RabbitMQ connection closed cleanly")
				}
			}

			// Reconnection loop
			for {
				slog.Info("Attempting to reconnect to RabbitMQ...")
				err := c.ReconnectRabbitMQ(uri)
				if err == nil {
					slog.Info("Reconnected to RabbitMQ successfully")
					
					// If post-reconnect hook is provided, run it
					if postReconnect != nil {
						newRmq := c.GetRabbitMQ()
						if err := postReconnect(newRmq.Channel); err != nil {
							slog.Error("Post-reconnect hook failed", "error", err)
							// Wait and retry reconnection
							time.Sleep(5 * time.Second)
							continue
						}
					}
					break
				}
				slog.Error("Failed to reconnect to RabbitMQ, retrying in 5 seconds...", "error", err)
				time.Sleep(5 * time.Second)
			}
		}
	}()
}

func (c *Connections) Ping() (error){
	res := c.RedisClient.Ping(context.Background())
	if res.Err() != nil {
		return res.Err()
	}
	if err := c.MongoClient.Ping(context.Background() , nil) ; err != nil{
		return err
	}
	return nil
}

// Close gracefully closes all active connections
func (c *Connections) Close() {
	if c.MongoClient != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := c.MongoClient.Disconnect(ctx); err != nil {
			slog.Error("Error closing MongoDB connection", "error", err)
		} else {
			slog.Info("Closed MongoDB connection cleanly")
		}
		cancel()
	}
	if c.RedisClient != nil {
		if err := c.RedisClient.Close(); err != nil {
			slog.Error("Error closing Redis connection", "error", err)
		} else {
			slog.Info("Closed Redis connection cleanly")
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.RabbitMQ != nil {
		if c.RabbitMQ.Channel != nil {
			c.RabbitMQ.Channel.Close()
		}
		if c.RabbitMQ.Conn != nil {
			c.RabbitMQ.Conn.Close()
		}
		slog.Info("Closed RabbitMQ connection cleanly")
	}
}
