package kafka

import "sync"

var instance *Kafka
var once sync.Once

func GetInstance() *Kafka {
	once.Do(func() {
		kafka := NewFromFile("kafka.txt")
		kafka.Sort()
		instance = &kafka
	})
	return instance
}
