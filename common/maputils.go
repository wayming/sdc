package common

import "sort"

// Define a constraint for types that support ordering
type Ordered interface {
	~int | ~float64 | ~string
}

func Exists[K comparable, V any](m map[K]V, e K) bool {
	_, ok := m[e]
	return ok
}

func Keys[K Ordered, V any](m map[K]V) []K {
	var keys []K
	for key := range m {
		keys = append(keys, key)
	}

	SortSlice(keys)
	return keys
}

// Function to sort a slice of ordered elements
func SortSlice[T Ordered](slice []T) {
	sort.Slice(slice, func(i, j int) bool {
		return slice[i] < slice[j]
	})
}
