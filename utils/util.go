package utils

import (
	"strconv"
	"strings"
)

func Str2int(str1, str2 string) int {
	split := strings.Split(str1, str2)
	num, _ := strconv.Atoi(split[1])
	return num
}

func Int2Str(num int) string {
	return strconv.Itoa(num)
}
