package utils

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

func Str2int(str1, str2 string) int {
	split := strings.Split(str1, str2)
	num, _ := strconv.Atoi(split[1])
	return num
}

func Int2Str(num int) string {
	return strconv.Itoa(num)
}

func ContainLimit(strs []string) (bool, int) {
	for _, str := range strs {
		str = strings.ToLower(str)
		if strings.Contains(str, "limit") {
			ss := strings.Split(str, "limit=")
			limit, _ := strconv.Atoi(ss[1])
			return true, limit
		}
	}
	return false, -1
}

func ContainPv(strs []string) bool {
	for _, str := range strs {
		str = strings.ToLower(str)
		if strings.Contains(str, "pv") {
			return true
		}
	}
	return false
}

func ContainValue(strs []string) (bool, string) {
	for _, str := range strs {
		str = strings.ToLower(str)
		if strings.Contains(str, "value") {
			ss := strings.Split(str, "value=")

			return true, ss[1]
		}
	}
	return false, ""
}

func ContainNolog(strs []string) bool {
	for _, str := range strs {
		str = strings.ToLower(str)
		if strings.Contains(str, "nolog") {
			return true
		}
	}
	return false
}

func IncrementLastCharASCII(s string) string {
	if len(s) == 0 {
		return s
	}

	b := []byte(s)
	lastChar := b[len(b)-1]
	// 检查最后一个字符是否为最大值
	if (lastChar >= 'a' && lastChar < 'z') ||
		(lastChar >= 'A' && lastChar < 'Z') ||
		(lastChar >= '0' && lastChar < '9') {
		b[len(b)-1]++
	} else {
		i := len(s) - 1
		for i >= 0 && s[i] >= '0' && s[i] <= '9' {
			i--
		}
		if i == len(s)-1 { // 没有数字后缀
			b[len(b)-1]++
			return string(b)
		}

		prefix := s[:i+1]
		numStr := s[i+1:]
		num, _ := strconv.Atoi(numStr)
		return prefix + strconv.Itoa(num+1)
	}
	return string(b)
}

func InitLog() (*log.Logger, *os.File, error) {
	now := time.Now()

	logFileName := fmt.Sprintf("tikvcli-%04d%02d%02d.log",
		now.Year(), now.Month(), now.Day())

	var logFile *os.File
	var err error

	// 检查日志文件是否存在
	if _, err := os.Stat(logFileName); err == nil {
		// 文件存在，以读写和追加模式打开文件
		logFile, err = os.OpenFile(logFileName, os.O_RDWR|os.O_APPEND, 0644)
	} else if os.IsNotExist(err) {
		// 文件不存在，创建文件
		logFile, err = os.OpenFile(logFileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	}
	if err != nil {
		return nil, logFile, fmt.Errorf("open log file err: %v", err)
	}

	return log.New(logFile, " [INFO] ", log.LstdFlags), logFile, nil
}
