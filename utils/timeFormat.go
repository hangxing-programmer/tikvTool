package utils

import (
	"fmt"
	"time"
)

//now := time.Now()
//physical := uint64(now.UnixMilli()) // 当前时间的毫秒级时间戳
//startTS := uint64(physical) << 18   // 左移 18 位（逻辑计数默认为 0）
//fmt.Printf("Current StartTS: %d\n", startTS)
//
//// 反向解析验证
//physical = startTS >> 18
//t := time.UnixMilli(int64(physical))
//fmt.Printf("Parsed Time: %s\n", t.Format("2006-01-02 15:04:05"))

//func TikvTimeFormat(startTS uint64) string {
//	return time.UnixMilli(int64(startTS >> 18)).Format("2006-01-02 15:04:05")
//}
//
//func TimeToTS(physical string) uint64 {
//	startTime, _ := time.ParseInLocation("2006-01-02 15:04:05", physical, time.UTC)
//	fmt.Println("start time: ", startTime)
//	return uint64(startTime.UnixMilli()) << 18
//}

var cst *time.Location

func init() {
	// 初始化时区（Asia/Shanghai）
	var err error
	cst, err = time.LoadLocation("Asia/Shanghai")
	if err != nil {
		panic(err)
	}
}
func TikvTimeFormat(startTS uint64) string {
	// 物理时间（毫秒）→ CST 时间字符串
	return time.UnixMilli(int64(startTS >> 18)).In(cst).Format("2006-01-02 15:04:05")
}

func TimeToTS(physical string) uint64 {
	// CST 时间字符串 → 物理时间（毫秒）→ StartTS
	startTime, err := time.ParseInLocation("2006-01-02 15:04:05", physical, cst)
	if err != nil {
		fmt.Println("解析失败:", err)
		return 0
	}
	return uint64(startTime.UnixMilli()) << 18
}
