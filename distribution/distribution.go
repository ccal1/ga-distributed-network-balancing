package distribution

import (
	"math/rand"
	"sort"
	"time"

	"github.com/ccal1/ga-distributed-network-balancing/kafka"
	"math"
)

var rnd = rand.New(rand.NewSource(time.Now().UnixNano()))

type Distribution struct {
	Topics       []topic
	BucketsTotal []int64
}

// Sums the values in one computer
func (d *Distribution) calculateTotals() {
	if len(d.Topics) == 0 {
		return
	}
	d.BucketsTotal = make([]int64, len(d.Topics[0].PartOrder))
	for _, topic := range d.Topics {
		for i := range topic.PartOrder {
			d.BucketsTotal[i] += int64(topic.getBucketPartitionSize(i))
		}
	}
}

func NewCleanDistribution() Distribution {
	k := *kafka.GetInstance()
	dist := Distribution{
		Topics:       make([]topic, len(k)),
		BucketsTotal: make([]int64, len(k[0].Partitions)),
	}

	for topicIdx := 0; topicIdx < len(k); topicIdx++ {
		dist.Topics[topicIdx].PartOrder = make([]int, len(k[topicIdx].Partitions))
		dist.Topics[topicIdx].topicPos = topicIdx
	}
	return dist
}


// Greedy Distribution
func NewGreedyDistribution() Distribution {
	k := *kafka.GetInstance()

	distribution := NewCleanDistribution()

	bucketSize := NewBucketTotalSlice()

	for topicIdx := len(k) - 1; topicIdx >= 0; topicIdx-- {
		for partitionPos, kafkaPartition := range k[topicIdx].Partitions {
			bucketSize[partitionPos].Total += int64(kafkaPartition.Value)
			bucket := bucketSize[partitionPos].Bucket
			distribution.BucketsTotal[bucket] = bucketSize[partitionPos].Total
			distribution.Topics[topicIdx].PartOrder[bucket] = partitionPos
		}
		sort.Sort(ByTotal(bucketSize))
	}
	return distribution
}

// Greedy Distribution
func NewStochasticGreedyDistribution(sortingDistributionChance float32) Distribution {
	k := *kafka.GetInstance()

	distribution := NewCleanDistribution()

	bucketSize := NewBucketTotalSlice()

	for topicIdx := len(k) - 1; topicIdx >= 0; topicIdx-- {
		for partitionPos, kafkaPartition := range k[topicIdx].Partitions {
			bucketSize[partitionPos].Total += int64(kafkaPartition.Value)
			bucket := bucketSize[partitionPos].Bucket
			distribution.BucketsTotal[bucket] = bucketSize[partitionPos].Total
			distribution.Topics[topicIdx].PartOrder[bucket] = partitionPos
		}

		if rnd.Float32() < sortingDistributionChance {
			sort.Sort(ByTotal(bucketSize))
		}
	}
	return distribution
}


// Average of the totals in the buckets
func (d *Distribution) avg() float64 {
	var total int64
	for _, bucket := range d.BucketsTotal {
		total += bucket
	}
	return float64(total) / float64(len(d.BucketsTotal))
}

// Standard Deviation of the totals in the buckets
func (d *Distribution) bucketStdDev() []float64 {
	avg := d.avg()

	stdDev := make([]float64, len(d.BucketsTotal))

	for i, bucket := range d.BucketsTotal {
		stdDev[i] = (float64(bucket) - avg) * (float64(bucket) - avg)
	}
	return stdDev
}

func (d *Distribution) bucketsChance() []float64 {
	stdDevs := d.bucketStdDev()
	maxDev := 0.0
	for _, dev := range stdDevs {
		maxDev = math.Max(maxDev, dev)
	}
	chances := make([]float64, len(stdDevs))

	for i, dev := range stdDevs {
		chances[i] = dev/(maxDev*2)
	}
	return chances
}

func (d *Distribution) chooseBuckets() []int {
	chances := d.bucketsChance()
	var buckets []int
	for i, chance := range chances {
		if rnd.Float64() < chance {
			buckets = append(buckets, i)
		}
	}
	return buckets
}

// Fitness from distribution: Difference between computer with bigger value and computer with smaller value
func (d *Distribution) GetFitness() int64 {
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

func (d *Distribution) ExponentialFitness() float64 {
	return 1.0 / (1.0 + float64(d.GetFitness()))
}

func (d *Distribution) chooseTopicsAndBuckets(numTopics int, numPartitions int) (topics []int, partitions []int) {
	topics = rnd.Perm(len(d.Topics))[0:numTopics]
	partitions = rnd.Perm(len(d.BucketsTotal))[0:numPartitions]
	//sort.Sort(sort.Reverse(sort.IntSlice(topics)))

	return
}

func (d *Distribution) shuffleTopicPartitions(topic topic, partitions []int) {
	// Shuffle partitions in topic chosen
	rnd.Shuffle(len(partitions), func(i, j int) {
		iPos := partitions[i]
		jPos := partitions[j]

		// Subtracting from the Total
		d.BucketsTotal[iPos] -= int64(topic.getBucketPartitionSize(iPos))
		d.BucketsTotal[jPos] -= int64(topic.getBucketPartitionSize(jPos))

		// Swap partitions
		topic.PartOrder[iPos], topic.PartOrder[jPos] = topic.PartOrder[jPos], topic.PartOrder[iPos]
		partitions[i], partitions[j] = partitions[j], partitions[i]

		// Summing with the Total
		d.BucketsTotal[iPos] += int64(topic.getBucketPartitionSize(iPos))
		d.BucketsTotal[jPos] += int64(topic.getBucketPartitionSize(jPos))
	})
}

func (d *Distribution) removeFromTotal(topics []int, buckets []int) {
	for _, topicIdx := range topics {
		topic := d.Topics[topicIdx]
		for _, bucket := range buckets {
			d.BucketsTotal[bucket] -= int64(topic.getBucketPartitionSize(bucket))
		}
	}
}

func (d *Distribution) rearrangeTopics(topics []int, buckets []int) {
	d.removeFromTotal(topics, buckets)

	//Other topics rearrange
	bucketSize := make([]BucketTotal, len(buckets))

	partitions := make([]int, len(buckets))

	// Set initial buckets total
	for i, bucket := range buckets {
		bucketSize[i].Bucket = bucket
		bucketSize[i].Total = d.BucketsTotal[bucket]
	}

	for _, topicIdx := range topics {
		sort.Sort(ByTotal(bucketSize))
		topic := d.Topics[topicIdx]

		for i, bucket := range buckets {
			partitions[i] = d.Topics[topicIdx].PartOrder[bucket]
		}

		sort.Ints(partitions)

		for i, bucket := range buckets {
			partition := partitions[i]
			bucketSize[i].Total += int64(topic.getPartitionSize(partition))
			d.BucketsTotal[bucket] = bucketSize[i].Total
			d.Topics[topicIdx].PartOrder[bucket] = partition
		}
	}
}

// Mutation GA
func (d *Distribution) mutateBucketsTopics(topics, partitions []int) {

	d.shuffleTopicPartitions(d.Topics[topics[0]], partitions)

	//Other topics rearrange
	d.rearrangeTopics(topics[1:], partitions)
}

func (d *Distribution) getTotal() int64 {
	var total int64
	for _, subtotal := range d.BucketsTotal {
		total += subtotal
	}
	return total
}

func (d *Distribution) newSubDistribution(topics []int, buckets []int) Distribution {
	distribution := Distribution{
		Topics:       make([]topic, len(topics)),
		BucketsTotal: make([]int64, len(d.BucketsTotal)),
	}

	for i, topicIdx := range topics {
		distribution.Topics[i].PartOrder = make([]int, len(d.Topics[topicIdx].PartOrder))
		copy(distribution.Topics[i].PartOrder, d.Topics[topicIdx].PartOrder)
		distribution.Topics[i].topicPos = d.Topics[topicIdx].topicPos
	}

	copy(distribution.BucketsTotal, d.BucketsTotal)

	return distribution
}

func (d *Distribution) mutateIfBetter() {
	// Number of topics to mutate
	shuffleTopicsSize := rnd.Intn(len(d.Topics)/2) + 1

	// Generate randomly topics and partitions to mutate
	topics := rnd.Perm(len(d.Topics))[:shuffleTopicsSize]
	buckets := d.chooseBuckets()

	subDistribution := d.newSubDistribution(topics, buckets)
	d.mutateBucketsTopics(topics, buckets)

	if d.GetFitness() > subDistribution.GetFitness() {
		copy(d.BucketsTotal, subDistribution.BucketsTotal)
		for _, topic := range subDistribution.Topics {
			d.Topics[topic.topicPos] = topic
		}
	}
}

func (d *Distribution) BucketsSubtotal(start, end int) []BucketTotal {
	subtotal := NewBucketTotalSlice()

	for topicIdx := start; topicIdx <= end; topicIdx++ {
		topic := d.Topics[topicIdx]

		for i, partition := range topic.PartOrder {
			subtotal[i].Total += int64(topic.getBucketPartitionSize(partition))
		}
	}
	return subtotal
}

func (d *Distribution) MutateIfBetterNTimes(times int){
	for i := 0 ; i < times; i++ {
		d.mutateIfBetter()
	}
}