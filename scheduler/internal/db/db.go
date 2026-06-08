package db

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/chahatsagarmain/distributed-web-crawler/common"
	"go.mongodb.org/mongo-driver/mongo"
)

const (
	BatchSize      = 100
	TickerInterval = 5 // 5 seconds
	DatabaseName   = "url_db"
	CollectionName = "crawled_urls"
)

type Batcher struct {
	*sync.Mutex
	data []common.UrlData
}

func NewBatcher() *Batcher {
	return &Batcher{
		Mutex: &sync.Mutex{},
		data:  make([]common.UrlData, 0),
	}
}

func (_ Batcher) Insert(conn *mongo.Client, documents []interface{}) {
	col := conn.Database(DatabaseName).Collection(CollectionName)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := col.InsertMany(ctx, documents)
	if err != nil {
		log.Printf("ERROR PERFORMING INSERT: %v", err)
	}
}

func (b *Batcher) BatchInsert(conn *mongo.Client, dbchan chan []byte) {
	ticker := time.NewTicker(time.Second * TickerInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			b.Lock()
			size := len(b.data)
			var documents []interface{}
			for _, val := range b.data {
				documents = append(documents, val)
			}
			b.data = b.data[:0]
			b.Unlock()

			if size > 0 {
				log.Printf("BATCH INSERT started of %v size", size)
				b.Insert(conn, documents)
			}
		case val := <-dbchan:
			log.Printf("INSERT DOCUMENT %v", string(val))
			var doc common.UrlData
			if err := json.Unmarshal(val, &doc); err != nil {
				log.Printf("ERROR: unmarshalling db document: %v", err)
				continue
			}
			b.Lock()
			b.data = append(b.data, doc)
			var documents []interface{}
			if len(b.data) >= BatchSize {
				for _, val := range b.data {
					documents = append(documents, val)
				}
				b.data = b.data[:0]
			}
			b.Unlock()
		}
	}
}
