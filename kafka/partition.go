package kafka

type Partition struct {
	Idx int
	Value int
}

type ByValue []Partition

func (p ByValue) Len() int {
	return len(p)
}

func (p ByValue) Swap(i,j int) {
	p[i], p[j] = p[j], p[i]
}

func (p ByValue) Less(i, j int) bool {
	return p[i].Value < p[j].Value
}
