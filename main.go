package main

import (
	"fmt"
	"github.com/ccal1/ga-distributed-network-balancing/kafka"
	"github.com/ccal1/ga-distributed-network-balancing/population"
)

const (
	EXECUTIONS = 30
	ITERATIONS = 500
	POPULATION_SIZE = 20
	POPULAYION_GENERATION_SORTING_CHANCE = 0.9
	MUTATIONS_IN_ITERATION = 20
)

func main() {
	k := *kafka.GetInstance()
	fmt.Println(k)

	populations := make([]population.Population, EXECUTIONS)
	for i := 0; i < EXECUTIONS; i++ {

		fmt.Printf("Execution: %v\n", i)
		populations[i] = generateAndEvolvePop()
	}

}

func generateAndEvolvePop() population.Population{
	pop := population.NewPopulation(POPULATION_SIZE, POPULAYION_GENERATION_SORTING_CHANCE)

	for i := 0; i < ITERATIONS; i++ {
		//fmt.Printf("Iteration: %v\n", i)
		pop.CreateChildEvolveAndReplaceIfBetter(MUTATIONS_IN_ITERATION)
		//fmt.Println("Best Fitness: ", pop.Best.GetFitness())
	}
	fmt.Println("Best Fitness: ", pop.Best.GetFitness())
	return pop
}