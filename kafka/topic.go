package kafka

type Topic struct {
	Name string
	Partitions []Partition
}

type ByDelta []Topic

func (t Topic) Delta() int {
	l := len(t.Partitions)
	if l <= 1 {
		return 0
	}
	return t.Partitions[l-1].Value - t.Partitions[0].Value
}

func (t ByDelta) Len() int {
	return len(t)
}

func (t ByDelta) Swap(i,j int) {
	t[i], t[j] = t[j], t[i]
}

func (t ByDelta) Less(i, j int) bool {
	return t[i].Delta() < t[j].Delta()
}