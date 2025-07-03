package main

import (
	"fmt"
	"github.com/peterh/liner"
	"github.com/pingcap/log"
	"github.com/tikv/client-go/v2/txnkv"
	"go.uber.org/zap"
	"strings"
	"tikv/actions"
	"tikv/base"
	"tikv/utils"
)

func main() {
	//utils.DataAdd()

	base.GlobalLogger, base.GLobalLogFile, _ = utils.InitLog()

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
		fmt.Printf("connect to tikv err:", err)
		return
	}
	fmt.Println("successful connected...")

	// 初始化命令行界面
	cli := &actions.TiKVClient{client}
	cli.StartCmd(line)

	defer base.GLobalLogFile.Close()
}
