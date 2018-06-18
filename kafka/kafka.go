package kafka

import (
	"math/rand"
	"sort"
	"time"
)

type Kafka []Topic

// Create new kafka randomly
func NewRandomKafka(partitions int, topics int) Kafka {
	seed := rand.New(rand.NewSource(time.Now().UnixNano()))
	rnd := rand.New(seed)
	kafka := make([]Topic, topics)
	for i := range kafka {
		topicSize := rnd.Intn(1000)
		kafka[i].Name = string(i)
		kafka[i].Partitions = make([]Partition, partitions)
		partitions := kafka[i].Partitions
		for j := range partitions {
			partitions[j].Idx = j
			partitions[j].Value = topicSize * rnd.Intn(100)
		}
	}
	return kafka
}


// Get the partitions value
func (k Kafka) GetTopicsPartition(topic, partition int) int {
	return k[topic].Partitions[partition].Value
}

// Sort the partitions by value and the kafka by delta
func (k Kafka) Sort() {
	for i := range k {
		topic := &k[i]
		sort.Sort(ByValue(topic.Partitions))
	}
	sort.Sort(ByDelta(k))
}
