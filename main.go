package main

import (
	"fmt"
	"io/ioutil"
	"math"
	"os"

	"github.com/ccal1/ga-distributed-network-balancing/kafka"
	"github.com/ccal1/ga-distributed-network-balancing/population"
)

const (
	EXECUTIONS                           = 30
	ITERATIONS                           = 500
	POPULATION_SIZE                      = 20
	POPULAYION_GENERATION_SORTING_CHANCE = 0.9
	MUTATIONS_IN_ITERATION               = 20
)

type Statistics struct {
	Mean   float64
	StdDev float64
	Best   int64
}

type Results struct {
	Mean       float64
	StdDev     float64
	BestMean   float64
	BestStdDev float64
}

type StatsResults struct {
	Vector        [][]Statistics
	ResultsVector []Results
}

func main() {
	k := kafka.GetInstance()
	fmt.Println(k)

	res := StatsResults{
		Vector:        make([][]Statistics, EXECUTIONS),
		ResultsVector: make([]Results, EXECUTIONS),
	}

	populations := make([]population.Population, EXECUTIONS)
	for i := 0; i < EXECUTIONS; i++ {

		fmt.Printf("Execution: %v\n", i)
		populations[i], res.Vector[i] = generateAndEvolvePop()

	}

	res.ResultsVector = generateResults(res)

	PrintResults("results.txt", res)

}

func generateAndEvolvePop() (population.Population, []Statistics) {
	pop := population.NewPopulation(POPULATION_SIZE, POPULAYION_GENERATION_SORTING_CHANCE)
	statVector := make([]Statistics, ITERATIONS)

	for i := 0; i < ITERATIONS; i++ {
		//fmt.Printf("Iteration: %v\n", i)
		pop.CreateChildEvolveAndReplaceIfBetter(MUTATIONS_IN_ITERATION)
		//fmt.Println("Best Fitness: ", pop.Best.GetFitness())
		statVector[i] = generateStatistics(pop)
	}
	fmt.Println("Best Fitness: ", pop.Best.GetFitness())
	return pop, statVector
}

func generateStatistics(p population.Population) Statistics {
	var stat Statistics
	var sum int64
	var stdDev float64

	sum = 0

	for i := 0; i < POPULATION_SIZE; i++ {
		sum += p.Distributions[i].GetFitness()
	}
	stat.Mean = float64(sum / POPULATION_SIZE)
	stat.Best = p.Best.GetFitness()

	for i := 0; i < POPULATION_SIZE; i++ {
		stdDev += (float64(p.Distributions[i].GetFitness()) - stat.Mean) * (float64(p.Distributions[i].GetFitness()) - stat.Mean)
	}

	stat.StdDev = math.Sqrt(stdDev / POPULATION_SIZE)
	return stat
}

func generateResults(sr StatsResults) []Results {
	resVector := make([]Results, ITERATIONS)

	for i := 0; i < ITERATIONS; i++ {
		for j := 0; j < EXECUTIONS; j++ {
			resVector[i].Mean += sr.Vector[j][i].Mean
			resVector[i].BestMean += float64(sr.Vector[j][i].Best)
		}
		resVector[i].Mean /= EXECUTIONS
		resVector[i].BestMean /= EXECUTIONS

		for j := 0; j < EXECUTIONS; j++ {
			resVector[i].StdDev += (sr.Vector[j][i].Mean - resVector[i].Mean) * (sr.Vector[j][i].Mean - resVector[i].Mean)
			resVector[i].BestStdDev += (float64(sr.Vector[j][i].Best) - resVector[i].BestMean) * (float64(sr.Vector[j][i].Best) - resVector[i].BestMean)
		}
		resVector[i].StdDev = math.Sqrt(resVector[i].StdDev / EXECUTIONS)
		resVector[i].BestStdDev = math.Sqrt(resVector[i].BestStdDev / EXECUTIONS)
	}

	return resVector
}

func PrintResults(fileName string, res StatsResults) error {
	str := ""
	str += fmt.Sprintf("Population Average:\n[")
	for i := 0; i < ITERATIONS; i++ {
		str += fmt.Sprintf("%v, ", res.ResultsVector[i].Mean)
	}
	str += fmt.Sprintf("]\nPopulation Standard Deviation:\n[")
	for i := 0; i < ITERATIONS; i++ {
		str += fmt.Sprintf("%v, ", res.ResultsVector[i].StdDev)
	}
	str += fmt.Sprintf("]\nBest Average:\n[")
	for i := 0; i < ITERATIONS; i++ {
		str += fmt.Sprintf("%v, ", res.ResultsVector[i].BestMean)
	}
	str += fmt.Sprintf("]\nBest Standard Deviation:\n[")
	for i := 0; i < ITERATIONS; i++ {
		str += fmt.Sprintf("%v, ", res.ResultsVector[i].BestStdDev)
	}
	str += fmt.Sprintf("]\n")
	return ioutil.WriteFile(fileName, []byte(str), os.ModePerm)
}
