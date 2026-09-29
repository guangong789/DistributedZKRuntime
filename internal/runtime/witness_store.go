package runtime

import (
	"encoding/json"
	"os"
)

type WitnessStore interface {
	Get(ref string) (int64, bool)
}

type MemoryWitnessStore struct {
	values map[string]int64
}

func NewMemoryWitnessStore() *MemoryWitnessStore {
	return &MemoryWitnessStore{
		values: make(map[string]int64),
	}
}

func (s *MemoryWitnessStore) Put(ref string, secret int64) {
	s.values[ref] = secret
}

func (s *MemoryWitnessStore) Get(ref string) (int64, bool) {
	secret, ok := s.values[ref]
	return secret, ok
}

func LoadMemoryWitnessStore(path string) (*MemoryWitnessStore, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var values map[string]int64
	if err := json.Unmarshal(data, &values); err != nil {
		return nil, err
	}

	store := NewMemoryWitnessStore()

	for ref, secret := range values {
		store.Put(ref, secret)
	}

	return store, nil
}
