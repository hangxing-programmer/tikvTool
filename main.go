package main

import "tikv/utils"

func main() {
	utils.DataAdd()

	//line := liner.NewLiner()
	//defer line.Close()
	//
	//line.SetCtrlCAborts(true)
	//
	//// 获取TiKV地址
	//fmt.Print(" Enter Tikv address cluster: ")
	//endpoints, err := line.Prompt("")
	//if err != nil {
	//	panic(err)
	//}
	//endpoints = strings.TrimSpace(endpoints)
	//addrs := strings.Split(endpoints, ",")
	//
	//log.SetLevel(zap.ErrorLevel)
	//client, err := txnkv.NewClient(addrs)
	//defer client.Close()
	//if err != nil {
	//	//log.Fatalf("连接失败: %v", err)
	//	fmt.Printf("连接失败,err:", err)
	//	return
	//}
	//fmt.Println("成功连接到TiKV集群...")
	//
	//// 初始化命令行界面
	//cli := &actions.TiKVClient{client}
	//cli.StartCmd(line)
	//
	//defer base.GLobalLogFile.Close()
}
