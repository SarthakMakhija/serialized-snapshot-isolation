package mvcc

import (
	"snapshot-isolation/mvcc/utils"
)

type SkiplistNode struct {
	key      VersionedKey
	value    Value
	forwards []*SkiplistNode
}

func newSkiplistNode(key VersionedKey, value Value, level int) *SkiplistNode {
	return &SkiplistNode{
		key:      key,
		value:    value,
		forwards: make([]*SkiplistNode, level),
	}
}

func (node *SkiplistNode) put(key VersionedKey, value Value, levelGenerator utils.LevelGenerator) bool {
	current := node
	positions := make([]*SkiplistNode, len(node.forwards))

	for level := len(node.forwards) - 1; level >= 0; level-- {
		for current.forwards[level] != nil && current.forwards[level].key.compare(key) < 0 {
			current = current.forwards[level]
		}
		positions[level] = current
	}

	current = current.forwards[0]
	if current == nil || current.key.compare(key) != 0 {
		newLevel := levelGenerator.Generate()
		newNode := newSkiplistNode(key, value, newLevel)
		for level := 0; level < newLevel; level++ {
			newNode.forwards[level] = positions[level].forwards[level]
			positions[level].forwards[level] = newNode
		}
		return true
	}
	return false
}

func (node *SkiplistNode) get(key VersionedKey) (Value, bool) {
	node, ok := node.matchingNode(key)
	if ok {
		return node.value, true
	}
	return emptyValue(), false
}

func (node *SkiplistNode) matchingNode(key VersionedKey) (*SkiplistNode, bool) {
	current := node
	lastNodeWithTheKey := current
	for level := len(node.forwards) - 1; level >= 0; level-- {
		for current.forwards[level] != nil && current.forwards[level].key.compare(key) < 0 {
			current = current.forwards[level]
			lastNodeWithTheKey = current
		}
	}
	current = current.forwards[0]
	if current != nil && current.key.matchesKeyPrefix(key.getKey()) {
		return current, true
	}
	if lastNodeWithTheKey != nil && lastNodeWithTheKey.key.matchesKeyPrefix(key.getKey()) {
		return lastNodeWithTheKey, true
	}
	return nil, false
}
