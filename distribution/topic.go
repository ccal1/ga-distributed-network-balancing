package distribution

import "github.com/ccal1/ga-distributed-network-balancing/kafka"

type topic struct {
	topicPos  int
	PartOrder []int
}

// Return the partition value
func (t topic) getPartitionSize(partition int) int {
	k := kafka.GetInstance()
	return k.GetTopicsPartition(t.topicPos, t.PartOrder[partition])
}
