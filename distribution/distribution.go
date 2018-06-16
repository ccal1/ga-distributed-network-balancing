package distribution

import (
	"fmt"
	"math/rand"
	"sort"
	"time"

	"github.com/ccal1/ga-distributed-network-balancing/kafka"
)

var rnd = rand.New(rand.NewSource(time.Now().UnixNano()))

type Distribution struct {
	Topics       []topic
	BucketsTotal []int64
}

type topic struct {
	topicPos  int
	partOrder []int
}

// Return the partition value
func (t topic) getPartitionSize(partition int) int {
	k := kafka.GetInstance()
	return k.GetTopicsPartition(t.topicPos, t.partOrder[partition])
}

// Sums the values in one computer
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

// Returns the bucket size
func (p byTotal) Len() int {
	return len(p)
}

// Swaps the partition in one line from kafka
func (p byTotal) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

// Compares two partitions in the same line from kafka
func (p byTotal) Less(i, j int) bool {
	return p[i].total > p[j].total
}

// Greedy Distribution
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

	for topicIdx := len(k) - 1; topicIdx >= 0; topicIdx-- {
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

// Average of the totals in the buckets
func (d Distribution) avg() float64 {
	var total int64
	for _, bucket := range d.BucketsTotal {
		total += bucket
	}
	return float64(total) / float64(len(d.BucketsTotal))
}

// Standard Deviation of the totals in the buckets
func (d Distribution) bucketStdDev() []float64 {
	avg := d.avg()

	stdDev := make([]float64, len(d.BucketsTotal))

	for i, bucket := range d.BucketsTotal {
		stdDev[i] = (float64(bucket) - avg) * (float64(bucket) - avg)
	}
	return stdDev
}

// Fitness from distribution: Difference between computer with bigger value and computer with smaller value
func (d Distribution) GetFitness() int64 {
	min := d.BucketsTotal[0]
	max := min
	for _, bucket := range d.BucketsTotal {
		if bucket < min {
			min = bucket
		}
		if bucket > max {
			max = bucket
		}
	}

	return max - min
}

func (d Distribution) chooseTopicsAndPartitions(numTopics int, numPartitions int) (topics []int, partitions []int) {
	topics = rnd.Perm(len(d.Topics))[0:numTopics]
	partitions = rnd.Perm(len(d.BucketsTotal))[0:numPartitions]

	return
}

func (d Distribution) shuffleTopicPartitions(topic topic, partitions []int) {

	// Shuffle partitions in topic chosen
	rnd.Shuffle(len(partitions), func(i, j int) {
		iPos := partitions[i]
		jPos := partitions[j]

		// Subtracting from the total
		d.BucketsTotal[iPos] -= int64(topic.getPartitionSize(iPos))
		d.BucketsTotal[jPos] -= int64(topic.getPartitionSize(jPos))

		// Swap partitions
		topic.partOrder[iPos], topic.partOrder[j] = topic.partOrder[j], topic.partOrder[i]

		// Summing with the total
		d.BucketsTotal[iPos] += int64(topic.getPartitionSize(iPos))
		d.BucketsTotal[jPos] += int64(topic.getPartitionSize(iPos))
	})
}

func (d Distribution) removeFromTotal(topics []int, buckets []int) {

	for _, topicIdx := range topics {
		topic := d.Topics[topicIdx]
		for _, bucket := range buckets {
			d.BucketsTotal[bucket] -= int64(topic.getPartitionSize(bucket))
		}
	}
}

func (d Distribution) rearrangeTopics(topics []int, buckets []int) {
	d.removeFromTotal(topics, buckets)

	//Other topics rearrange
	bucketSize := make([]bucketTotal, len(buckets))

	partitions := make([]int, len(buckets))

	for i, bucket := range buckets {
		bucketSize[i].bucket = bucket
		bucketSize[i].total = d.BucketsTotal[bucket]
	}

	for _, topicIdx := range topics {
		sort.Sort(byTotal(bucketSize))
		topic := d.Topics[topicIdx]

		for i, bucket := range buckets {
			partitions[i] = d.Topics[topicIdx].partOrder[bucket]
		}

		sort.Ints(partitions)

		for i, bucket := range buckets {
			partition := partitions[i]
			bucketSize[i].total += int64(topic.getPartitionSize(partition))
			d.BucketsTotal[bucket] = bucketSize[i].total
			d.Topics[topicIdx].partOrder[bucket] = partition
		}
		fmt.Println(d.Topics[topicIdx])
	}
}

// Mutation GA
func (d Distribution) mutateBucketsTopics(topics, partitions []int) {
	d.shuffleTopicPartitions(d.Topics[topics[0]], partitions)

	//Other topics rearrange
	d.rearrangeTopics(topics[1:], partitions)
}

func (d Distribution) newSubDistribution(topics []int, buckets []int) Distribution {
	distribution := Distribution{
		Topics:       make([]topic, len(topics)),
		BucketsTotal: make([]int64, len(d.BucketsTotal)),
	}

	for i, topicIdx := range topics {
		distribution.Topics[i].partOrder = make([]int, len(d.Topics[topicIdx].partOrder))
		copy(distribution.Topics[i].partOrder, d.Topics[topicIdx].partOrder)
		distribution.Topics[i].topicPos = d.Topics[topicIdx].topicPos
	}

	copy(distribution.BucketsTotal, d.BucketsTotal)

	return distribution
}

func (d Distribution) mutateIfBetter() {

	// Number of partitions to swap
	shuffleBucketsSize := rnd.Intn(len(d.BucketsTotal)) + 1

	// Number of topics to mutate
	shuffleTopicsSize := rnd.Intn(len(d.Topics)) + 1

	// Generate randomly topics and partitions to mutate
	topics, buckets := d.chooseTopicsAndPartitions(shuffleTopicsSize, shuffleBucketsSize)

	subDistribution := d.newSubDistribution(topics, buckets)
	d.mutateBucketsTopics(topics, buckets)

	if d.GetFitness() > subDistribution.GetFitness() {
		copy(d.BucketsTotal, subDistribution.BucketsTotal)
		for _, topic := range subDistribution.Topics {
			d.Topics[topic.topicPos] = topic
		}
	}
}
