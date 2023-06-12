package utils

import (
	"math/rand"
	"time"
)

type LevelGenerator struct {
	maxLevel   uint8
	skipFactor int
	random     *rand.Rand
}

func NewLevelGenerator(maxLevel uint8) LevelGenerator {
	random := rand.New(rand.NewSource(time.Now().UnixNano()))

	return LevelGenerator{
		maxLevel:   maxLevel,
		skipFactor: 2,
		random:     random,
	}
}

func (levelGenerator LevelGenerator) Generate() uint8 {
	level := uint8(1)
	newRandom := levelGenerator.random.Float64()
	for level < levelGenerator.GetMaxLevel() && newRandom < 1.0/float64(levelGenerator.skipFactor) {
		level = level + 1
		newRandom = rand.Float64()
	}
	return level
}

func (levelGenerator LevelGenerator) GetMaxLevel() uint8 {
	return levelGenerator.maxLevel
}
