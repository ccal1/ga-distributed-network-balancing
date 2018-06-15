package distribution

import (
	"fmt"
	"math/rand"
	"sort"

	"github.com/ccal1/ga-distributed-network-balancing/kafka"
)

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

// Mutation GA
func (d Distribution) mutateBucketsTopics(distribution Distribution) Distribution {

	topicChosen := rand.Intn(len(distribution.Topics))

	shufflePositionsSize := rand.Intn(len(distribution.BucketsTotal))
	shufflePositions := make([]int, shufflePositionsSize)
	positionsList := make(map[int]int)

	for i := 0; i < shufflePositionsSize; {
		position := rand.Intn(len(distribution.BucketsTotal))
		_, exists := positionsList[position]
		if !exists {
			positionsList[position] = 1
			shufflePositions[i] = position
			i++
		}
	}

	positionsUsed := make(map[int]int)
	newPositions := make([]int, shufflePositionsSize)

	for i := 0; i < shufflePositionsSize; {
		position := rand.Intn(shufflePositionsSize)
		_, exists := positionsUsed[positionsList[position]]
		if !exists {
			newPositions[i] = shufflePositions[position]
			positionsUsed[shufflePositions[position]] = 1
			i++
		}
	}

	tempValues := make([]int, len(distribution.BucketsTotal))

	for i := 0; i < shufflePositionsSize; i++ {
		tempValues[i] = distribution.Topics[topicChosen].partOrder[newPositions[i]]
	}
	for i := 0; i < shufflePositionsSize; i++ {
		distribution.Topics[topicChosen].partOrder[shufflePositions[i]] = tempValues[i]
	}

	bucketSize := make([]bucketTotal, len(distribution.Topics[0].partOrder))

	for i := range bucketSize {
		bucketSize[i].bucket = i
	}

	for topicIdx := len(distribution.Topics) - 1; topicIdx >= 0; topicIdx-- {
		distribution.Topics[topicIdx].partOrder = make([]int, len(distribution.Topics[topicIdx].partOrder))

		for partitionPos, kafkaPartition := range distribution.Topics[topicIdx].partOrder {
			bucketSize[partitionPos].total += int64(kafkaPartition)
			if topicIdx != topicChosen {
				bucket := bucketSize[partitionPos].bucket
				distribution.BucketsTotal[bucket] = bucketSize[partitionPos].total
				distribution.Topics[topicIdx].partOrder[bucket] = partitionPos
			}
		}
		fmt.Println(distribution.Topics[topicIdx])
		sort.Sort(byTotal(bucketSize))
	}
	return distribution
}
