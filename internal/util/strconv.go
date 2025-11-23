package util

import "strconv"

func Atoi(s string) (int64, error) {
	return strconv.ParseInt(s, 10, 64)
}
