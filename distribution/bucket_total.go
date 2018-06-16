package distribution

import "github.com/ccal1/ga-distributed-network-balancing/kafka"

type BucketTotal struct {
	Total  int64
	Bucket int
}

type ByTotal []BucketTotal

// Returns the Bucket size
func (p ByTotal) Len() int {
	return len(p)
}

// Swaps the partition in one line from kafka
func (p ByTotal) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

// Compares two partitions in the same line from kafka
func (p ByTotal) Less(i, j int) bool {
	return p[i].Total > p[j].Total
}

type ReverseByTotal []BucketTotal

// Returns the Bucket size
func (p ReverseByTotal) Len() int {
	return len(p)
}

// Swaps the partition in one line from kafka
func (p ReverseByTotal) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

// Compares two partitions in the same line from kafka
func (p ReverseByTotal) Less(i, j int) bool {
	return p[i].Total < p[j].Total
}


func NewBucketTotalSlice() []BucketTotal {
	k := *kafka.GetInstance()
	bucketsTotal := make([]BucketTotal, len(k[0].Partitions))

	for i := range bucketsTotal {
		bucketsTotal[i].Bucket = i
	}
	return bucketsTotal
}
