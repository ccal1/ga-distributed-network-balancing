package kafka

import "sync"


var instance *Kafka
var once sync.Once

func GetInstance() *Kafka {
	once.Do(func() {
		kafka := NewRandomKafka(25,100)
		kafka.Sort()
		instance = &kafka
	})
	return instance
}


