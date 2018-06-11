package main

import "fmt"
import "github.com/ccal1/ga-distributed-network-balancing/kafka"

func main() {
	k := *kafka.GetInstance()
	fmt.Println(k)
	for _, topic := range k {
		fmt.Println(topic.Delta())
	}
}