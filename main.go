package main

import (
	"fmt"
	"github.com/peterh/liner"
	"github.com/pingcap/log"
	"github.com/tikv/client-go/v2/txnkv"
	"go.uber.org/zap"
	"strings"
	"tikv/actions"
)

func main() {
	//utils.DataAdd()

	line := liner.NewLiner()
	defer line.Close()

	line.SetCtrlCAborts(true)

	// 获取TiKV地址
	fmt.Print(" Enter Tikv address cluster: ")
	endpoints, err := line.Prompt("")
	if err != nil {
		panic(err)
	}
	endpoints = strings.TrimSpace(endpoints)
	addrs := strings.Split(endpoints, ",")

	log.SetLevel(zap.ErrorLevel)
	client, err := txnkv.NewClient(addrs)
	defer client.Close()
	if err != nil {
		//log.Fatalf("连接失败: %v", err)
		fmt.Printf("连接失败,err:", err)
		return
	}
	fmt.Println("成功连接到TiKV集群...")

	// 初始化命令行界面
	cli := &actions.TiKVClient{client}
	cli.StartCmd(line)

	//Current StartTS: 458995024769843200
	//Current StartTS: 458995024527360000
	//Current StartTS: 459002574274560000
	//Parsed Time: 2025-06-26-16:37:45
	//fmt.Println(utils.TikvTimeFormat(458995024769843200))
	//fmt.Println(utils.TimeToTS("2025-06-26 16:37:45"))

	//now := time.Now()
	//physical := uint64(now.UnixMilli()) // 当前时间的毫秒级时间戳
	//startTS := physical << 18           // 左移 18 位（逻辑计数默认为 0）
	//fmt.Printf("Current StartTS: %d\n", startTS)
	//
	//// 反向解析验证
	//physical = startTS >> 18
	//t := time.UnixMilli(int64(physical))
	//fmt.Printf("Parsed Time: %s\n", t.Format("2006-01-02 15:04:05"))
}
