package utils

import "cmp"

// LastElement returns the last element of a slice of any type
func LastElement[T any](slice []T) T {
	if len(slice) == 0 {
		return *new(T)
	}
	return slice[len(slice)-1]
}

// Pop returns the last element of a slice and the slice without the last element
func Pop[T any](slice []T) (T, []T) {
	if len(slice) == 0 {
		return *new(T), slice
	}
	return slice[len(slice)-1], slice[:len(slice)-1]
}

// Max returns the maximum value of a slice
func Max[T cmp.Ordered](slice []T) T {
	if len(slice) == 0 {
		return *new(T)
	}
	max := slice[0]
	for _, v := range slice {
		if cmp.Compare(v, max) > 0 {
			max = v
		}
	}
	return max
}

// Min returns the minimum value of a slice
func Min[T cmp.Ordered](slice []T) T {
	if len(slice) == 0 {
		return *new(T)
	}
	min := slice[0]
	for _, v := range slice {
		if cmp.Compare(v, min) < 0 {
			min = v
		}
	}
	return min
}

// Range returns a slice of integers from start to end
func Range(start, end int64) []int64 {
	slice := make([]int64, end-start)
	for i := range slice {
		slice[i] = start + int64(i)
	}
	return slice
}

// Keys returns the keys of a map
func Keys[K comparable, V any](m map[K]V) []K {
	keys := make([]K, 0)
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// Values returns the values of a map
func Values[K comparable, V any](m map[K]V) []V {
	values := make([]V, 0)
	for _, v := range m {
		values = append(values, v)
	}
	return values
}
