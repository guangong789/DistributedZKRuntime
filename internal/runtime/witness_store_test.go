package runtime

import (
	"os"
	"path/filepath"
	"testing"
)

func TestMemoryWitnessStorePutGet(t *testing.T) {
	store := NewMemoryWitnessStore()
	store.Put("secret-001", 42)

	secret, ok := store.Get("secret-001")
	if !ok || secret != 42 {
		t.Fatalf("Get(secret-001) = (%d, %t), want (42, true)", secret, ok)
	}
}

func TestMemoryWitnessStoreMissingRef(t *testing.T) {
	store := NewMemoryWitnessStore()

	if secret, ok := store.Get("missing"); ok {
		t.Fatalf("Get(missing) = (%d, true), want a missing ref", secret)
	}
}

func TestMemoryWitnessStoreOverwrite(t *testing.T) {
	store := NewMemoryWitnessStore()
	store.Put("secret-001", 42)
	store.Put("secret-001", 84)

	secret, ok := store.Get("secret-001")
	if !ok || secret != 84 {
		t.Fatalf("Get(secret-001) after overwrite = (%d, %t), want (84, true)", secret, ok)
	}
}

func TestLoadMemoryWitnessStore(t *testing.T) {
	path := filepath.Join(t.TempDir(), "witnesses.json")
	if err := os.WriteFile(path, []byte(`{"secret-001":42,"secret-002":99,"zero":0,"negative":-7}`), 0o600); err != nil {
		t.Fatalf("write witness file: %v", err)
	}

	store, err := LoadMemoryWitnessStore(path)
	if err != nil {
		t.Fatalf("load witness file: %v", err)
	}
	for ref, want := range map[string]int64{
		"secret-001": 42,
		"secret-002": 99,
		"zero":       0,
		"negative":   -7,
	} {
		if got, ok := store.Get(ref); !ok || got != want {
			t.Errorf("Get(%q) = (%d, %t), want (%d, true)", ref, got, ok, want)
		}
	}
}

func TestLoadMemoryWitnessStoreMissingFile(t *testing.T) {
	store, err := LoadMemoryWitnessStore(filepath.Join(t.TempDir(), "missing.json"))
	if err == nil || store != nil {
		t.Fatalf("missing file: store=%v error=%v", store, err)
	}
}

func TestLoadMemoryWitnessStoreMalformedJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "witnesses.json")
	if err := os.WriteFile(path, []byte(`{"secret-001":`), 0o600); err != nil {
		t.Fatalf("write malformed witness file: %v", err)
	}
	store, err := LoadMemoryWitnessStore(path)
	if err == nil || store != nil {
		t.Fatalf("malformed JSON: store=%v error=%v", store, err)
	}
}

func TestLoadMemoryWitnessStoreEmptyObject(t *testing.T) {
	path := filepath.Join(t.TempDir(), "witnesses.json")
	if err := os.WriteFile(path, []byte(`{}`), 0o600); err != nil {
		t.Fatalf("write empty witness file: %v", err)
	}
	store, err := LoadMemoryWitnessStore(path)
	if err != nil || store == nil {
		t.Fatalf("load empty object: store=%v error=%v", store, err)
	}
	if value, ok := store.Get("secret-001"); ok {
		t.Fatalf("empty store returned witness %d", value)
	}
}
