package population

import (
	"github.com/ccal1/ga-distributed-network-balancing/distribution"
	"math/rand"
	"sort"
	"github.com/ccal1/ga-distributed-network-balancing/kafka"
)

type Population struct {
	Distributions []distribution.Distribution
	Best distribution.Distribution
}

func NewPopulation(size int, initializationSortingChance float32) Population {
	pop := Population{
		Distributions: make([]distribution.Distribution, size),
	}

	for i := range pop.Distributions {
		pop.Distributions[i] = distribution.NewStochasticGreedyDistribution(initializationSortingChance)
	}
	pop.Best = pop.Distributions[0]

	for _, dist := range pop.Distributions {
		if pop.Best.GetFitness() > dist.GetFitness() {
			pop.Best = dist
		}
	}

	return pop
}

func (p Population) rouletteParentsSelection() (distribution.Distribution, distribution.Distribution) {
	accumulatedFitness := make([]float64, len(p.Distributions))
	sumFitness := 0.0

	for i, dist := range p.Distributions {
		if i > 0 {
			accumulatedFitness[i] = accumulatedFitness[i-1]
		}
		accumulatedFitness[i] += dist.ExponentialFitness()
		sumFitness = accumulatedFitness[i]
	}

	randomChoices := []float64{rand.Float64() * sumFitness, rand.Float64() * sumFitness}

	sort.Float64s(randomChoices)
	choicePos := 0

	parents := make([]distribution.Distribution, 2)

	for i, fit := range accumulatedFitness {
		if randomChoices[choicePos] <= fit {
			parents[choicePos] = p.Distributions[i]
			choicePos++
		}
	}

	return parents[0], parents[1]
}

// Performs crossover with a ordered part from the father and another one from the mother
func (p Population) crossOver(father, mother distribution.Distribution) distribution.Distribution {
	k :=  *kafka.GetInstance()
	splitPos := rand.Intn(len(k))

	child := distribution.NewCleanDistribution()

	fatherSubtotal := father.BucketsSubtotal(0, splitPos)
	sort.Sort(distribution.ByTotal(fatherSubtotal))

	motherSubtotal := mother.BucketsSubtotal(splitPos + 1, len(k))
	sort.Sort(distribution.ReverseByTotal(motherSubtotal))

	for i := range child.BucketsTotal {
		child.BucketsTotal[i] = fatherSubtotal[i].Total + motherSubtotal[i].Total
	}

	for topicIdx := 0; topicIdx <= splitPos ; topicIdx++ {
		for i, bucket := range fatherSubtotal {
			child.Topics[topicIdx].PartOrder[i] = father.Topics[topicIdx].PartOrder[bucket.Bucket]
		}
	}

	for topicIdx := splitPos + 1; topicIdx <= len(k) ; topicIdx++ {
		for i, bucket := range motherSubtotal {
			child.Topics[topicIdx].PartOrder[i] = mother.Topics[topicIdx].PartOrder[bucket.Bucket]
		}
	}

	return child
}

