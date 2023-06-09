package utils

import "testing"

func TestShouldGenerateLevelGreaterThanEqualTo1(t *testing.T) {
	levelGenerator := NewLevelGenerator(10)
	level := levelGenerator.Generate()

	if level < 1 {
		t.Fatalf("Expected generated level to be greater than or equal to 1 but received %v", level)
	}
}

func TestShouldGenerateLevelGreaterThanEqualTo1_MultipleRuns(t *testing.T) {
	levelGenerator := NewLevelGenerator(10)
	for count := 1; count <= 1000; count++ {
		level := levelGenerator.Generate()

		if level < 1 {
			t.Fatalf("Expected generated level to be greater than or equal to 1 but received %v", level)
		}
	}
}
