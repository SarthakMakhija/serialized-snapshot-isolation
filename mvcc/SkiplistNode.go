package mvcc

import (
	"serialized-snapshot-isolation/mvcc/utils"
)

// SkiplistNode represents a node in the SkipList.
// Each node contains the key/value pair and an array of forward pointers.
// SkipListNode maintains VersionedKeys: each key has a version which is the commitTimestamp.
// A sample Level0 of SkipListNode with HDD as the key can be represented as:
// HDD1: Hard Disk -> HDD2: Hard disk -> HDD5: Hard disk drive. Here, 1, 2, and 5 are the versions of the key HDD.
type SkiplistNode struct {
	key      VersionedKey
	value    Value
	forwards []*SkiplistNode
}

// newSkiplistNode creates a new instance of SkiplistNode.
func newSkiplistNode(key VersionedKey, value Value, level uint8) *SkiplistNode {
	return &SkiplistNode{
		key:      key,
		value:    value,
		forwards: make([]*SkiplistNode, level),
	}
}

// putOrUpdate puts or updates the value corresponding to the incoming key.
func (node *SkiplistNode) putOrUpdate(key VersionedKey, value Value, levelGenerator utils.LevelGenerator) bool {
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
		for level := uint8(0); level < newLevel; level++ {
			newNode.forwards[level] = positions[level].forwards[level]
			positions[level].forwards[level] = newNode
		}
		return true
	}
	return false
}

// get returns a pair of (Value, bool) for the incoming key.
// It returns (Value, true) if the value exists for the incoming key, else (nil, false).
// get attempts to find the key where:
// 1. the version of the key < version of the incoming key &&
// 2. the key prefixes match.
// KeyPrefix is the actual key or the byte slice.
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
	if current != nil && current.key.matchesKeyPrefix(key.getKey()) {
		return current, true
	}
	if lastNodeWithTheKey != nil && lastNodeWithTheKey.key.matchesKeyPrefix(key.getKey()) {
		return lastNodeWithTheKey, true
	}
	return nil, false
}
