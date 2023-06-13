package utils

import (
	"math/rand"
	"time"
)

// LevelGenerator generates a new level for the SkipListNode.
// The generated level is greater than or equal to 1 and less than the max level.
type LevelGenerator struct {
	maxLevel   uint8
	skipFactor int
	random     *rand.Rand
}

// NewLevelGenerator creates a new instance of the LevelGenerator.
// There is one instance of LevelGenerator in the mvcc.MemTable.
func NewLevelGenerator(maxLevel uint8) LevelGenerator {
	random := rand.New(rand.NewSource(time.Now().UnixNano()))

	return LevelGenerator{
		maxLevel:   maxLevel,
		skipFactor: 2,
		random:     random,
	}
}

// Generate generates a new level.
func (levelGenerator LevelGenerator) Generate() uint8 {
	level := uint8(1)
	newRandom := levelGenerator.random.Float64()
	for level < levelGenerator.GetMaxLevel() && newRandom < 1.0/float64(levelGenerator.skipFactor) {
		level = level + 1
		newRandom = rand.Float64()
	}
	return level
}

// GetMaxLevel returns the maximum level.
func (levelGenerator LevelGenerator) GetMaxLevel() uint8 {
	return levelGenerator.maxLevel
}
