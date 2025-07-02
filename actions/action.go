package actions

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/peterh/liner"
	"github.com/tikv/client-go/v2/txnkv"
	"github.com/tikv/client-go/v2/txnkv/transaction"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"tikv/utils"
	"time"
)

type TiKVClient struct {
	Client *txnkv.Client
}
type Data struct {
	Owner       string `json:"owner"`
	LockTime    int64  `json:"lockTime"`
	MaxDuration int64  `json:"maxDuration"`
}

func (c *TiKVClient) StartCmd(line *liner.State) {
	for {
		input, err := line.Prompt("TiKVClient> ")
		if err != nil {
			if errors.Is(err, liner.ErrPromptAborted) {
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
			if len(cmd) == 2 {
				c.handleListAll(cmd[1], false)
			} else if len(cmd) == 3 { // 提供前缀,有value
				if cmd[2] == "-pv" {
					c.handleListAll(cmd[1], true)
				} else if len(cmd) == 3 && strings.Contains(cmd[2], "-limit=") {
					c.handleListRange(cmd[1], "", false, utils.Str2int(cmd[2], "-limit="))
				} else {
					c.handleListRange(cmd[1], cmd[2], false, -1)
				}
			} else if len(cmd) == 4 { // 有参数时范围读取,有value
				if strings.Contains(cmd[2], "-limit") && strings.Contains(cmd[3], "-pv") {
					split := strings.Split(cmd[2], "=")
					limit, err := strconv.Atoi(split[1])
					if err != nil {
						fmt.Println("输入-limit参数有误")
						continue
					}
					c.handleListRange(cmd[1], "", true, limit)
				} else if strings.Contains(cmd[3], "-limit") && !strings.Contains(cmd[3], "-pv") {
					split := strings.Split(cmd[3], "=")
					limit, err := strconv.Atoi(split[1])
					if err != nil {
						fmt.Println("输入-limit参数有误")
						continue
					}
					c.handleListRange(cmd[1], cmd[2], false, limit)
				} else {
					fmt.Println("使用方法: ll <prefixKey> [endKey] -limit=n -pv")
				}
			} else if len(cmd) == 5 {
				if strings.Contains(cmd[4], "-pv") && strings.Contains(cmd[3], "-limit") {
					split := strings.Split(cmd[3], "=")
					limit, err := strconv.Atoi(split[1])
					if err != nil {
						fmt.Println("输入-limit参数有误")
						continue
					}
					c.handleListRange(cmd[1], cmd[2], true, limit)
				} else {
					fmt.Println("使用方法: ll <prefixKey> [endKey] -limit=n -pv")
				}
			} else {
				fmt.Println("使用方法: ll <prefixKey> [endKey] -limit=n -pv")
			}
		case "set":
			if len(cmd) < 3 {
				fmt.Println("使用方法: set <key> <value>")
				continue
			}
			c.HandleSet(cmd[1], strings.Join(cmd[2:], " "))
		case "del":
			if len(cmd) < 2 {
				fmt.Println("使用方法: del <key>; del <startKey> <endKey>")
				continue
			} else if len(cmd) == 2 {
				fmt.Printf("是否确认删除 key=%s? (yes/no): ", cmd[1])
				var confirm string
				_, err := fmt.Scan(&confirm)
				if err != nil {
					fmt.Printf("读取用户输入失败: %v\n", err)
					continue
				}
				if confirm != "yes" {
					continue
				} else {
					fmt.Println("键已删除")
					c.handleDelete(cmd[1])
				}
			} else if len(cmd) == 3 {
				c.handleDelRange(cmd[1], cmd[2])
			} else if len(cmd) == 5 {
				lockTime, _ := strconv.Atoi(cmd[4])
				maxDuration, _ := strconv.Atoi(cmd[3])
				c.handleDeleteLock(cmd[1], cmd[2], int64(maxDuration), int64(lockTime))
			} else {
				fmt.Println("使用方法: del <key>; del <startKey> <endKey>; del <lockKey> owner maxDuration lockTime")
			}

		case "find":
			if len(cmd) < 4 {
				fmt.Println("使用方法: find <prefixKey> [endKey] -value=xxx -limit=n -pv")
				continue
			} else if len(cmd) == 6 && strings.Contains(cmd[5], "pv") {
				c.findLike(cmd[1], cmd[2], strings.Split(cmd[3], "-value=")[1], true, utils.Str2int(cmd[4], "-limit="))
			} else if len(cmd) == 4 && strings.Contains(cmd[3], "-limit") {
				c.findLike(cmd[1], "", strings.Split(cmd[2], "-value=")[1], false, utils.Str2int(cmd[3], "-limit="))
			} else if len(cmd) == 5 && strings.Contains(cmd[3], "-limit") && strings.Contains(cmd[4], "-pv") {
				c.findLike(cmd[1], "", strings.Split(cmd[2], "-value=")[1], true, utils.Str2int(cmd[3], "-limit="))
			} else if len(cmd) == 5 && !strings.Contains(cmd[3], "-pv") {
				c.findLike(cmd[1], cmd[2], strings.Split(cmd[3], "-value=")[1], false, utils.Str2int(cmd[4], "-limit="))
			} else {
				fmt.Println("使用方法: find <prefixKey> [endKey] -value=xxx -limit=n -pv")
			}
		case "exit":
			return
		case "count":
			if len(cmd) < 2 {
				fmt.Println("使用方法: count <prefixKey> [endKey]")
				continue
			} else if len(cmd) == 3 && strings.Contains(cmd[2], "-value=") {
				c.handleCount(cmd[1], "", strings.Split(cmd[2], "-value=")[1])
			} else if len(cmd) == 4 && strings.Contains(cmd[3], "-value=") {
				c.handleCount(cmd[1], cmd[2], strings.Split(cmd[3], "-value=")[1])
			} else if len(cmd) == 2 {
				c.handleCount(cmd[1], "", "")
			} else {
				fmt.Println("使用方法: count <prefixKey> [endKey]")
			}
		default:
			fmt.Println("可用命令: get, ll, exit, set, del, find, count")
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

func (c *TiKVClient) handleListAll(start string, pv bool) {

	// 创建中断信号通道
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	err := c.executeTxn(func(txn *transaction.KVTxn) error {
		iter, err := txn.Iter([]byte(start), []byte(utils.IncrementLastCharASCII(start)))
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
				if pv {
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

func (c *TiKVClient) handleListRange(key1, key2 string, pv bool, limit int) {

	// 如果key2为空，则计算key1的下一个键
	if key2 == "" {
		if !strings.Contains(key1, "/") {
			key1 = key1 + "/"
		}
		key2 = utils.IncrementLastCharASCII(key1)
	} else {
		key2 = utils.IncrementLastCharASCII(key2)
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
				if pv {
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

func (c *TiKVClient) HandleSet(key, value string) {
	err := c.executeTxn(func(txn *transaction.KVTxn) error {
		return txn.Set([]byte(key), []byte(value))
	})

	if err != nil {
		fmt.Printf("操作失败: %v\n", err)
		return
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
}

func (c *TiKVClient) findLike(key1, key2, value string, pv bool, limit int) {
	// 创建中断信号通道
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	if key2 == "" {
		key2 = utils.IncrementLastCharASCII(key1)
	} else {
		key2 = utils.IncrementLastCharASCII(key2)
	}
	err := c.executeTxn(func(txn *transaction.KVTxn) error {
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
				k := iter.Key()
				v := iter.Value()
				if pv && strings.Contains(string(k), value) {
					fmt.Printf("%s", string(k))
					fmt.Printf("  Value = %s\n", string(v))
					count++
				} else {
					if strings.Contains(string(k), value) {
						fmt.Printf("%s\n", string(k))
						count++
					}
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
	startTime := time.Now()

	if strings.Contains(start, ":") && strings.Contains(end, ":") {
		s1 := start[strings.LastIndex(start, "/")+1:]
		s2 := end[strings.LastIndex(end, "/")+1:]
		s1 = s1[:strings.LastIndex(s1, "-")] + " " + s1[strings.LastIndex(s1, "-")+1:]
		s2 = s2[:strings.LastIndex(s2, "-")] + " " + s2[strings.LastIndex(s2, "-")+1:]
		startTS := utils.TimeToTS(s1)
		endTS := utils.TimeToTS(s2)
		start = start[0:strings.LastIndex(start, "/")+1] + strconv.Itoa(int(startTS)) + "1000000"
		end = end[0:strings.LastIndex(end, "/")+1] + strconv.Itoa(int(endTS)) + "1000001"
	}
	batchSize := 3000
	processedInBatch := 0
	startKey := []byte(start)
	endKey := []byte(utils.IncrementLastCharASCII(end))

	for {
		txn, err := c.Client.Begin()
		if err != nil {
			fmt.Printf("事务开启失败: %v\n", err)
			return
		}
		defer txn.Rollback()

		iter, err := txn.Iter(startKey, endKey)
		if err != nil {
			fmt.Printf("迭代err: %v\n", err)
			return
		}
		defer iter.Close()

		processedInBatch = 0
		for iter.Valid() && processedInBatch < batchSize {
			select {
			case <-sigCh:
				fmt.Println("\n操作已取消")
				return
			default:
				fmt.Printf("是否确认删除 key=%s? (yes/no): ", iter.Key())
				var confirm string
				_, err := fmt.Scan(&confirm)
				if err != nil {
					fmt.Printf("读取用户输入失败: %v\n", err)
					continue
				}
				if confirm != "yes" {
					if err = iter.Next(); err != nil {
						fmt.Printf("iter.Next err: %v\n", err)
						break
					}
					continue
				}
				err = txn.Delete(iter.Key())
				if err != nil {
					fmt.Printf("删除key=%s失败: %v\n", iter.Key(), err)
				} else {
					deletedTotal++
					processedInBatch++
				}
				if err = iter.Next(); err != nil {
					fmt.Printf("iter.Next err: %v\n", err)
					break
				}
			}
		}

		// 提交当前批次
		if processedInBatch > 0 {
			err = txn.Commit(context.Background())
			if err != nil {
				fmt.Printf("事务提交失败: %v\n", err)
				return
			}
			fmt.Printf("已删除批次: %d, 总计已删除: %d\n", processedInBatch, deletedTotal)

			if iter.Valid() {
				startKey = append(iter.Key(), 0)
			} else {
				break
			}
		} else {
			break
		}
	}

	fmt.Println("已删除总计:", deletedTotal, "耗时:", time.Since(startTime))
}

func (c *TiKVClient) handleDeleteLock(key, owner string, maxDuration, lockTime int64) {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh)
	deletedTotal := 0
	startTime := time.Now()

	startKey := key + "/Data/Lock"

	for {
		txn, err := c.Client.Begin()
		if err != nil {
			fmt.Printf("事务开启失败: %v\n", err)
			return
		}
		defer txn.Rollback()

		iter, err := txn.Iter([]byte(startKey), []byte(utils.IncrementLastCharASCII(startKey)))
		if err != nil {
			fmt.Printf("迭代err: %v\n", err)
			return
		}
		defer iter.Close()

		batchSize := 1000
		processedInBatch := 0
		for iter.Valid() && processedInBatch < batchSize {
			select {
			case <-sigCh:
				fmt.Println("\n操作已取消")
				return
			default:
				var result []byte
				err = c.executeTxn(func(txn *transaction.KVTxn) error {
					val, err := txn.Get(context.Background(), iter.Key())
					result = val
					return err
				})
				var data Data
				err = json.Unmarshal(result, &data)
				if err != nil {
					log.Fatalf("JSON 解析失败: %v", err)
				}

				if strings.Compare(data.Owner, owner) == 0 && data.MaxDuration == maxDuration && data.LockTime > lockTime {
					err = txn.Delete(iter.Key())
					if err != nil {
						fmt.Printf("删除key=%s失败: %v\n", iter.Key(), err)
					} else {
						deletedTotal++
						processedInBatch++
					}
				}
				if err = iter.Next(); err != nil {
					fmt.Printf("iter.Next err: %v\n", err)
					break
				}
			}
		}

		// 提交当前批次
		if processedInBatch > 0 {
			err = txn.Commit(context.Background())
			if err != nil {
				fmt.Printf("事务提交失败: %v\n", err)
				return
			}
			fmt.Printf("已删除批次: %d, 总计已删除: %d\n", processedInBatch, deletedTotal)
		} else {
			break
		}
	}
	fmt.Println("已删除总计:", deletedTotal, "耗时:", time.Since(startTime))
}

func (c *TiKVClient) handleCount(key1, key2, value string) {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	if key2 == "" {
		key2 = utils.IncrementLastCharASCII(key1)
	} else {
		key2 = utils.IncrementLastCharASCII(key2)
	}

	err := c.executeTxn(func(txn *transaction.KVTxn) error {
		iter, err := txn.Iter([]byte(key1), []byte(key2))
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
				if value != "" && strings.Contains(string(key), value) {
					count++
				} else if value == "" {
					count++
				}

			}
			if err := iter.Next(); err != nil {
				fmt.Printf("迭代失败: %v\n", err)
				break
			}
		}
		fmt.Println(count)
		return nil
	})
	if err != nil {
		fmt.Printf("操作失败: %v\n", err)
		return
	}

}
