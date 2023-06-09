package mvcc

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestVersionedKeyWithKey(t *testing.T) {
	versionedKey := newVersionedKey([]byte("storage"), 1)
	assert.Equal(t, []byte("storage"), versionedKey.getKey())
}

func TestVersionedKeyWithVersion(t *testing.T) {
	versionedKey := newVersionedKey([]byte("storage"), 1)
	assert.Equal(t, uint64(1), versionedKey.getVersion())
}

func TestSameVersionedKeyCompareEquals(t *testing.T) {
	versionedKey := newVersionedKey([]byte("storage"), 1)
	otherVersionedKey := newVersionedKey([]byte("storage"), 1)
	assert.Equal(t, 0, versionedKey.compare(otherVersionedKey))
}

func TestSameVersionedKeyLesserInVersion(t *testing.T) {
	versionedKey := newVersionedKey([]byte("storage"), 1)
	otherVersionedKey := newVersionedKey([]byte("storage"), 2)
	assert.Equal(t, -1, versionedKey.compare(otherVersionedKey))
}

func TestSameVersionedKeyGreaterInVersion(t *testing.T) {
	versionedKey := newVersionedKey([]byte("storage"), 2)
	otherVersionedKey := newVersionedKey([]byte("storage"), 1)
	assert.Equal(t, 1, versionedKey.compare(otherVersionedKey))
}

func TestDifferentVersionedKeysWithTheOriginalKeyLesser(t *testing.T) {
	versionedKey := newVersionedKey([]byte("disk"), 0)
	otherVersionedKey := newVersionedKey([]byte("storage"), 0)
	assert.Equal(t, -1, versionedKey.compare(otherVersionedKey))
}

func TestDifferentVersionedKeysWithTheOriginalKeyGreater(t *testing.T) {
	versionedKey := newVersionedKey([]byte("storage"), 0)
	otherVersionedKey := newVersionedKey([]byte("disk"), 0)
	assert.Equal(t, 1, versionedKey.compare(otherVersionedKey))
}

func TestMatchesKeyPrefix(t *testing.T) {
	versionedKey := newVersionedKey([]byte("storage"), 1)
	otherVersionedKey := newVersionedKey([]byte("storage"), 1)
	assert.Equal(t, true, versionedKey.matchesKeyPrefix(otherVersionedKey.getKey()))
}

func TestDoesNotMatchKeyPrefix(t *testing.T) {
	versionedKey := newVersionedKey([]byte("storage"), 1)
	otherVersionedKey := newVersionedKey([]byte("HDD"), 1)
	assert.Equal(t, false, versionedKey.matchesKeyPrefix(otherVersionedKey.getKey()))
}
