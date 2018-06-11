package distribution

import (
	"github.com/ccal1/ga-distributed-network-balancing/kafka"
	"fmt"
	"sort"
)

type Distribution struct {
	Topics       []topic
	BucketsTotal []int64
}

type topic struct {
	topicPos  int
	partOrder []int
}

func (t topic) getPartitionSize(partition int) int {
	k := kafka.GetInstance()
	return k.GetTopicsPartition(t.topicPos, t.partOrder[partition])
}

func (d *Distribution) calculateTotals() {
	if len(d.Topics) == 0 {
		return
	}
	d.BucketsTotal = make([]int64, len(d.Topics[0].partOrder))
	for _, topic := range d.Topics {
		for i := range topic.partOrder {
			d.BucketsTotal[i] += int64(topic.getPartitionSize(i))
		}
	}
}

type bucketTotal struct {
	total  int64
	bucket int
}

type byTotal []bucketTotal

func (p byTotal) Len() int {
	return len(p)
}

func (p byTotal) Swap(i,j int) {
	p[i], p[j] = p[j], p[i]
}

func (p byTotal) Less(i, j int) bool {
	return p[i].total > p[j].total
}

func NewGreedyDistribution() Distribution {
	k := *kafka.GetInstance()

	distribution := Distribution{
		Topics:       make([]topic, len(k)),
		BucketsTotal: make([]int64, len(k[0].Partitions)),
	}

	bucketSize := make([]bucketTotal, len(k[0].Partitions))

	for i := range bucketSize {
		bucketSize[i].bucket = i
	}

	for topicIdx := len(k)-1; topicIdx >= 0; topicIdx-- {
		distribution.Topics[topicIdx].partOrder = make([]int, len(k[topicIdx].Partitions))

		for partitionPos, kafkaPartition := range k[topicIdx].Partitions {
			bucketSize[partitionPos].total += int64(kafkaPartition.Value)
			bucket := bucketSize[partitionPos].bucket
			distribution.BucketsTotal[bucket] = bucketSize[partitionPos].total
			distribution.Topics[topicIdx].partOrder[bucket] = partitionPos
		}
		fmt.Println(k[topicIdx])
		sort.Sort(byTotal(bucketSize))
	}
	return distribution
}

func (d Distribution) avg() float64 {
	var total int64
	for _, bucket := range d.BucketsTotal {
		total += bucket
	}
	return float64(total)/float64(len(d.BucketsTotal))
}

func (d Distribution) bucketStdDev() []float64 {
	avg := d.avg()

	stdDev := make([]float64, len(d.BucketsTotal))

	for i, bucket := range d.BucketsTotal {
		stdDev[i] = (float64(bucket) - avg) * (float64(bucket) - avg)
	}
	return stdDev
}

func (d Distribution) GetFitness() int64 {
	min := d.BucketsTotal[0]
	max := min
	for _, bucket := range d.BucketsTotal {
		if bucket < min { min = bucket }
		if bucket > max { max = bucket }
	}

	return max - min
}

func (d Distribution) mutateBucketsTopics(buckets, topics []int) {

}
