package kafka

import (
	"math/rand"
	"sort"
	"time"
	"fmt"
	"io/ioutil"
	"os"
	"io"
	"encoding/json"
	"strings"
	"strconv"
)

type Kafka []Topic

// Create new kafka randomly
func NewRandomKafka(partitions int, topics int) Kafka {
	seed := rand.New(rand.NewSource(time.Now().UnixNano()))
	rnd := rand.New(seed)
	kafka := make([]Topic, topics)
	for i := range kafka {
		topicSize := rnd.Intn(1000)
		kafka[i].Name = strconv.Itoa(i)
		kafka[i].Partitions = make([]Partition, partitions)
		partitions := kafka[i].Partitions
		for j := range partitions {
			partitions[j].Idx = j
			partitions[j].Value = topicSize * rnd.Intn(100)
		}
	}
	return kafka
}

func (k *Kafka) Write(w io.Writer) {
	b, _ := json.Marshal(*k)
	w.Write(b)
}

const(
	topicSeparator = "SEPARATOR FOR TOPIC"
	partitionSeparator = "PARTITION SEPARATOR"
)

func (k Kafka) ToFile(fileName string) error {
	str := ""
	str += fmt.Sprintf("%v %v\n", len(k), len(k[0].Partitions))
	for _, topic := range k {
		str += topicSeparator
		str += topic.Name + "\n"
		str += partitionSeparator
		for _, partition := range topic.Partitions {
			str += fmt.Sprintf("%v %v\n", partition.Idx, partition.Value)
		}
	}

	return ioutil.WriteFile(fileName, []byte(str), os.ModePerm)
}

func NewFromFile(fileName string) Kafka {
	bytes, err := ioutil.ReadFile(fileName)
	if err != nil {
		panic(err)
	}
	str := string(bytes)
	strs := strings.Split(str, topicSeparator)

	var topicsSize, partitionsSize int
	fmt.Sscanf(strs[0],"%d %d", &topicsSize, &partitionsSize)
	kafka := make([]Topic, topicsSize)
	for i, topic := range strs[1:] {
		nameAndPartitions := strings.Split(topic, partitionSeparator)
		kafka[i].Name = nameAndPartitions[0]
		kafka[i].Partitions = make([]Partition, partitionsSize)

		for j, partition := range strings.Split(nameAndPartitions[1], "\n") {
			if partition == "" {
				break
			}
			fmt.Sscanf(partition, "%d %d", &(kafka[i].Partitions[j].Idx), &(kafka[i].Partitions[j].Value))
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
