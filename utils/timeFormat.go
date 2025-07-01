package utils

import (
	"context"
	"fmt"
	"github.com/tikv/client-go/v2/txnkv"
	"strconv"
	"strings"
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

func DataAdd() {
	client, _ := txnkv.NewClient(strings.Split("10.0.11.33:2379,10.0.11.34:2379,10.0.11.35:2379", ","))
	txn, _ := client.Begin()
	baseTime, err := time.Parse("2006-01-02 15:04:05", "2025-07-01 10:10:10")
	if err != nil {
		fmt.Println("Error parsing time:", err)
		return
	}
	str := "OS/T03/test/"
	for i := 0; i < 10000; i++ {
		ts := baseTime.Add(time.Duration(i) * time.Second)
		tsString := ts.Format("2006-01-02 15:04:05")
		_ = txn.Set([]byte(str+strconv.FormatInt(int64(TimeToTS(tsString)), 10)+"1000000"), []byte("test"))
	}
	_ = txn.Commit(context.Background())
}
