package utils

import (
	"math/rand"
	"time"
)

type LevelGenerator struct {
	maxLevel   int
	skipFactor int
	random     *rand.Rand
}

func NewLevelGenerator(maxLevel int) LevelGenerator {
	random := rand.New(rand.NewSource(time.Now().UnixNano()))

	return LevelGenerator{
		maxLevel:   maxLevel,
		skipFactor: 2,
		random:     random,
	}
}

func (levelGenerator LevelGenerator) Generate() int {
	level := 1
	newRandom := levelGenerator.random.Float64()
	for level < levelGenerator.GetMaxLevel() && newRandom < 1.0/float64(levelGenerator.skipFactor) {
		level = level + 1
		newRandom = rand.Float64()
	}
	return level
}

func (levelGenerator LevelGenerator) GetMaxLevel() int {
	return levelGenerator.maxLevel
}
