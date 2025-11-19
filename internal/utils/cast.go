package utils

// Convert bool to int64
func BoolToInt64(b bool) int64 {
	if b {
		return 1
	}
	return 0
}

// Convert int64 to bool
func Int64ToBool(i int64) bool {
	return i != 0
}
