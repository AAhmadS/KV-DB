package hash

import (
	"testing"
)

func TestHashKeyDeterministic(t *testing.T) {
	a := HashKey("hello")
	b := HashKey("hello")
	if a != b {
		t.Fatalf("HashKey not deterministic: %d != %d", a, b)
	}
}

func TestHashKeyKnownVector(t *testing.T) {
	// FNV-1a 32 for "" is the offset basis.
	if got := HashKey(""); got != 2166136261 {
		t.Fatalf("empty-string hash = %d, want offset basis 2166136261", got)
	}
}

func TestHashKeyDistribution(t *testing.T) {
	if HashKey("key-a") == HashKey("key-b") {
		t.Fatal("distinct keys hashed identically")
	}
}

func TestPartitionForKeyRange(t *testing.T) {
	const n = 8
	seen := map[int]bool{}
	for i := 0; i < 200; i++ {
		p := PartitionForKey("key-"+string(rune('a'+i%26))+string(rune('0'+i%10)), n)
		if p < 0 || p >= n {
			t.Fatalf("partition %d out of range [0,%d)", p, n)
		}
		seen[p] = true
	}
	if len(seen) < 2 {
		t.Fatal("all keys landed in one partition; hash distribution broken")
	}
}

func TestPartitionForKeyStable(t *testing.T) {
	for _, k := range []string{"user:1", "order:42", "session:xyz"} {
		if PartitionForKey(k, 4) != PartitionForKey(k, 4) {
			t.Fatalf("unstable partition for %q", k)
		}
	}
}

func TestPartitionForKeyInvalidN(t *testing.T) {
	if PartitionForKey("k", 0) != 0 {
		t.Fatal("expected 0 for non-positive partition count")
	}
}
