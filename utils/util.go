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

func IncrementLastCharASCII(s string) string {
	if len(s) == 0 {
		return s // 如果字符串为空，直接返回
	}
	// 将字符串转换为字节切片
	b := []byte(s)
	// 检查最后一个字符是否为最大值
	if b[len(b)-1] == '\xff' || b[len(b)-1] == 'z' {
		return string(b)
	} else {
		// 如果不是最大值，直接加 1
		b[len(b)-1]++
	}
	return string(b)
}
