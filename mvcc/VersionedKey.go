package mvcc

import (
	"bytes"
)

// VersionedKey represents a key with a version.
// Versioned is used as a key inside Skiplist based memtable which acts as an in-memory store.
// Versioned key has a version field that is the commitTimestamp of the key which is assigned by txn.Oracle.
type VersionedKey struct {
	key     []byte
	version uint64
}

// NewVersionedKey creates a new instance of the VersionedKey
func NewVersionedKey(key []byte, version uint64) VersionedKey {
	return VersionedKey{key: key, version: version}
}

// emptyVersionedKey creates an empty VersionedKey.
// This is used to create the sentinel node of Skiplist.
func emptyVersionedKey() VersionedKey {
	return VersionedKey{}
}

// getKey returns the key from the VersionedKey
func (versionedKey VersionedKey) getKey() []byte {
	return versionedKey.key
}

// getVersion returns the version from the VersionedKey
func (versionedKey VersionedKey) getVersion() uint64 {
	return versionedKey.version
}

// compare the two VersionedKeys.
// Two VersionedKeys are equal if their contents and the versions are same.
// If two VersionedKeys are equal in their content, then their version is used to
// get the comparison result.
func (versionedKey VersionedKey) compare(other VersionedKey) int {
	comparisonResult := bytes.Compare(versionedKey.getKey(), other.getKey())
	if comparisonResult == 0 {
		thisVersion, otherVersion := versionedKey.getVersion(), other.getVersion()
		if thisVersion == otherVersion {
			return 0
		}
		if thisVersion < otherVersion {
			return -1
		}
		return 1
	}
	return comparisonResult
}

// matchesKeyPrefix returns true if the key part of the VersionedKey matches the incoming key.
func (versionedKey VersionedKey) matchesKeyPrefix(key []byte) bool {
	return bytes.Compare(versionedKey.getKey(), key) == 0
}

// asString returns the string of the key part.
func (versionedKey VersionedKey) asString() string {
	return string(versionedKey.key)
}
