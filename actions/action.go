package actions

import (
	"context"
	"fmt"
	"github.com/peterh/liner"
	"github.com/tikv/client-go/v2/txnkv"
	"github.com/tikv/client-go/v2/txnkv/transaction"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
)

type TiKVClient struct {
	Client *txnkv.Client
}

func (c *TiKVClient) StartCmd(line *liner.State) {
	for {
		input, err := line.Prompt("TiKVClient> ")
		if err != nil {
			if err == liner.ErrPromptAborted {
				return
			}
			fmt.Println("读取输入失败:", err)
			continue
		}

		line.AppendHistory(input)
		cmd := strings.Fields(strings.TrimSpace(input))
		if len(cmd) == 0 {
			continue
		}

		switch cmd[0] {
		case "get":
			if len(cmd) < 2 {
				fmt.Println("使用方法: get <key>")
				continue
			}
			c.handleGet(cmd[1])
		case "ll":
			if len(cmd) == 3 { // 提供前缀,有value
				if cmd[2] == "-json" {
					c.handleListAll(cmd[1], cmd[2])
				} else if strings.Contains(cmd[2], "-limit") {
					limit, _ := strconv.Atoi(strings.Split(cmd[2], "=")[1])
					c.handleListRange(cmd[1], "", "", limit)
				} else {
					c.handleListRange(cmd[1], cmd[2], "", -1)
				}
			} else if len(cmd) == 4 { // 有参数时范围读取,有value
				if strings.Contains(cmd[3], "-limit") && strings.Contains(cmd[2], "-json") {
					split := strings.Split(cmd[3], "=")
					limit, err := strconv.Atoi(split[1])
					if err != nil {
						fmt.Println("输入-limit参数有误")
						return
					}
					c.handleListRange(cmd[1], "", cmd[2], limit)
				} else if strings.Contains(cmd[2], "-limit") && strings.Contains(cmd[3], "-json") {
					split := strings.Split(cmd[2], "=")
					atoi, err := strconv.Atoi(split[1])
					if err != nil {
						fmt.Println("输入-limit参数有误")
						return
					}
					c.handleListRange(cmd[1], "", cmd[3], atoi)
				} else if strings.Contains(cmd[3], "-limit") {
					split := strings.Split(cmd[3], "=")
					atoi, err := strconv.Atoi(split[1])
					if err != nil {
						fmt.Println("输入-limit参数有误")
						return
					}
					c.handleListRange(cmd[1], cmd[2], "", atoi)
				} else if strings.Contains(cmd[3], "-json") {
					c.handleListRange(cmd[1], cmd[2], cmd[3], -1)
				}
			} else if len(cmd) == 5 {
				split := strings.Split(cmd[4], "=")
				atoi, err := strconv.Atoi(split[1])
				if err != nil {
					fmt.Println("输入-limit参数有误")
					return
				}
				c.handleListRange(cmd[1], cmd[2], cmd[3], atoi)
			} else {
				fmt.Println("使用方法: ll <startKey> <endKey> -json -limit=n")
			}
		case "set":
			if len(cmd) < 3 {
				fmt.Println("使用方法: set <key> <value>")
				continue
			}
			c.handleSet(cmd[1], strings.Join(cmd[2:], " "))
		case "del":
			if len(cmd) < 2 {
				fmt.Println("使用方法: del <key>")
				continue
			} else if len(cmd) == 2 {
				c.handleDelete(cmd[1])
			} else if len(cmd) == 3 {
				c.handleDelRange(cmd[1], cmd[2]+"0")
			}

		case "find":
			if len(cmd) < 2 {
				fmt.Println("使用方法: find <key>")
				continue
			}
			c.findLike(cmd[1])
		case "exit":
			return
		default:
			fmt.Println("可用命令: get, ll, exit, set, del, find,")
		}
	}
}

func (c *TiKVClient) executeTxn(fn func(txn *transaction.KVTxn) error) error {
	txn, err := c.Client.Begin()
	if err != nil {
		return fmt.Errorf("事务启动失败: %w", err)
	}

	if err := fn(txn); err != nil {
		_ = txn.Rollback()
		return err
	}

	if err := txn.Commit(context.Background()); err != nil {
		return fmt.Errorf("事务提交失败: %w", err)
	}
	return nil
}

func (c *TiKVClient) handleGet(key string) {
	var result []byte
	err := c.executeTxn(func(txn *transaction.KVTxn) error {
		val, err := txn.Get(context.Background(), []byte(key))
		result = val
		return err
	})

	if err != nil {
		fmt.Printf("查询失败: %v\n", err)
		return
	}
	fmt.Printf("value = %s\n", string(result))
}

func (c *TiKVClient) handleListAll(start, json string) {

	// 创建中断信号通道
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	err := c.executeTxn(func(txn *transaction.KVTxn) error {
		iter, err := txn.Iter([]byte(start), nil)
		if err != nil {
			fmt.Printf("创建迭代器失败: %v\n", err)
			return nil
		}
		defer iter.Close()

		var count int
		for iter.Valid() {
			select {
			case <-sigCh:
				fmt.Println("\n操作已取消")
				return nil
			default:
				key := iter.Key()
				value := iter.Value()

				// 格式化显示
				if json == "-json" {
					fmt.Printf("%s", string(key))
					fmt.Printf("	Value = %s\n", string(value))
				} else {
					fmt.Printf("%s\n", string(key))
				}
			}
			count++
			if err := iter.Next(); err != nil {
				fmt.Printf("迭代失败: %v\n", err)
				break
			}

		}
		fmt.Println("-------------------")
		fmt.Printf("共找到 %d 条记录\n", count)
		return nil
	})
	if err != nil {
		fmt.Printf("操作失败: %v\n", err)
		return
	}
}

func (c *TiKVClient) handleListRange(key1, key2, json string, limit int) {

	// 如果key2为空，则计算key1的下一个键
	if key2 == "" {
		key2Bytes := []byte(key1)
		for i := len(key2Bytes) - 1; i >= 0; i-- {
			if key2Bytes[i] < 255 {
				key2Bytes[i]++
				break
			}
			key2Bytes[i] = 0
		}
		key2 = string(key2Bytes)
	}

	// 创建中断信号通道
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	err := c.executeTxn(func(txn *transaction.KVTxn) error {
		// 左闭右开
		iter, err := txn.Iter([]byte(key1), []byte(key2))
		if err != nil {
			fmt.Printf("创建迭代器失败: %v\n", err)
			return err
		}
		defer iter.Close()

		var count int
		for iter.Valid() {
			if count >= limit && limit > 0 {
				break
			}
			select {
			case <-sigCh:
				fmt.Println("\n操作已取消")
				return nil
			default:
				key := iter.Key()
				value := iter.Value()

				// 格式化显示
				if json == "-json" {
					fmt.Printf("%s", string(key))
					fmt.Printf("	Value = %s\n", string(value))
				} else {
					fmt.Printf("%s\n", string(key))
				}
			}
			count++
			if err := iter.Next(); err != nil {
				fmt.Printf("迭代失败: %v\n", err)
				break
			}
		}
		fmt.Println("-------------------")
		fmt.Printf("共找到 %d 条记录\n", count)
		return nil
	})
	if err != nil {
		fmt.Printf("操作失败: %v\n", err)
		return
	}
}

func (c *TiKVClient) handleSet(key, value string) {
	for i := 0; i < 1000; i++ {
		err := c.executeTxn(func(txn *transaction.KVTxn) error {
			return txn.Set([]byte(key+strconv.Itoa(i)), []byte(value))
		})

		if err != nil {
			fmt.Printf("操作失败: %v\n", err)
			return
		}
	}
	fmt.Println("键值已更新")
}

func (c *TiKVClient) handleDelete(key string) {
	err := c.executeTxn(func(txn *transaction.KVTxn) error {
		return txn.Delete([]byte(key))
	})
	if err != nil {
		fmt.Printf("删除失败: %v\n", err)
		return
	}
	fmt.Println("键已删除")
}

func (c *TiKVClient) findLike(key string) {
	// 创建中断信号通道
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	err := c.executeTxn(func(txn *transaction.KVTxn) error {
		iter, err := txn.Iter(nil, nil)
		if err != nil {
			fmt.Printf("创建迭代器失败: %v\n", err)
			return err
		}
		defer iter.Close()

		var count int
		for iter.Valid() {
			select {
			case <-sigCh:
				fmt.Println("\n操作已取消")
				return nil
			default:
				k := iter.Key()
				v := iter.Value()

				if strings.Contains(string(k), key) {
					fmt.Printf("%s", string(k))
					fmt.Printf(" Value = %s\n", string(v))
					count++
				}
			}
			if err := iter.Next(); err != nil {
				fmt.Printf("迭代失败: %v\n", err)
				break
			}
		}
		fmt.Println("-------------------")
		fmt.Printf("共找到 %d 条记录\n", count)
		return nil
	})
	if err != nil {
		fmt.Printf("操作失败: %v\n", err)
		return
	}
}

func (c *TiKVClient) handleDelRange(start, end string) {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh)
	deletedTotal := 0

	txn, err := c.Client.Begin()
	if err != nil {
		fmt.Printf("事务开启失败: %v\n", err)
		return
	}
	iter, err := txn.Iter([]byte(start), []byte(end))
	if err != nil {
		fmt.Printf("迭代err: %v\n", err)
		return
	}
	defer iter.Close()
	for iter.Valid() {
		select {
		case <-sigCh:
			fmt.Println("\n操作已取消")
			fmt.Printf("总计删除:%d", deletedTotal)
			return
		default:
			err = txn.Delete(iter.Key())
			if err != nil {
				fmt.Printf("删除key=%s失败: %v\n", iter.Key(), err)
			} else {
				deletedTotal++
			}
			if err = iter.Next(); err != nil {
				fmt.Printf("iter.Next err: %v\n", err)
				break
			}
			if deletedTotal >= 100 {
				err = txn.Commit(context.Background())
				if err != nil {
					fmt.Printf("事务提交失败: %v\n", err)
				} else {
					fmt.Println("已删除:100")
					txn, err = c.Client.Begin()
					if err != nil {
						fmt.Printf("事务二次开启失败: %v\n", err)
						return
					}
				}
			}
		}
	}
	fmt.Println("总计删除:", deletedTotal)

	//err := c.executeTxn(func(txn *transaction.KVTxn) error {
	//	iter, err := txn.Iter([]byte(start), []byte(end))
	//	if err != nil {
	//		fmt.Printf("迭代 key 失败: %v", err)
	//		return nil
	//	}
	//	for iter.Valid() {
	//		select {
	//		case <-sigCh:
	//			fmt.Println("\n操作已取消")
	//			fmt.Printf("总计删除:%d", deletedTotal)
	//			return nil
	//		default:
	//			err = txn.Delete(iter.Key())
	//			if err != nil {
	//				fmt.Printf("删除key:%s失败,err:%v\n", iter.Key(), err)
	//			}
	//			deletedTotal++
	//			if err = iter.Next(); err != nil {
	//				fmt.Printf("iter.Next err: %v\n", err)
	//				break
	//			}
	//			if deletedTotal >= 10000 {
	//				fmt.Printf("已删除: %d", deletedTotal)
	//				start = string(iter.Key())
	//				break
	//			}
	//		}
	//	}
	//	return err
	//})
	//if err != nil {
	//	fmt.Printf("操作失败: %v\n", err)
	//	return
	//}
	//if deletedTotal > 0 {
	//	fmt.Printf("总删除: %d\n", deletedTotal)
	//}

}
