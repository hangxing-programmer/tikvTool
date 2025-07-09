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
	"tikv/base"
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

var cmdStr []string

func (c *TiKVClient) StartCmd(line *liner.State) {
	for {
		input, err := line.Prompt("TiKVClient> ")
		if err != nil {
			if errors.Is(err, liner.ErrPromptAborted) {
				return
			}
			fmt.Println("input err:", err)
			continue
		}

		line.AppendHistory(input)
		cmd := strings.Fields(strings.TrimSpace(input))
		if len(cmd) == 0 {
			continue
		}

		cmdStr = cmd

		switch cmd[0] {
		case "get":
			if len(cmd) < 2 {
				fmt.Println("usage: get <key>")
				continue
			}
			c.handleGet(cmd[1])
		case "ll":
			containLimit, limit := utils.ContainLimit(cmd)
			containPv := utils.ContainPv(cmd)
			if len(cmd) == 2 {
				c.handleListAll(cmd[1], false)
			} else if len(cmd) == 3 { // 提供前缀,有value
				if containPv {
					c.handleListAll(cmd[1], true)
				} else if len(cmd) == 3 && containLimit {
					c.handleListRange(cmd[1], "", false, limit)
				} else {
					c.handleListRange(cmd[1], cmd[2], false, -1)
				}
			} else if len(cmd) == 4 { // 有参数时范围读取,有value
				if containLimit && containPv {
					c.handleListRange(cmd[1], "", true, limit)
				} else if containLimit && !containPv {
					c.handleListRange(cmd[1], cmd[2], false, limit)
				} else if !containLimit && containPv {
					c.handleListRange(cmd[1], cmd[2], true, -1)
				} else {
					fmt.Println("usage: ll <prefixKey> [endKey] -limit=n -pv")
				}
			} else if len(cmd) == 5 {
				if containPv && containLimit {
					c.handleListRange(cmd[1], cmd[2], true, limit)
				} else {
					fmt.Println("usage: ll <prefixKey> [endKey] -limit=n -pv")
				}
			} else {
				fmt.Println("usage: ll <prefixKey> [endKey] -limit=n -pv")
			}
		case "set":
			if len(cmd) < 3 {
				fmt.Println("usage: set <key> <value>")
				continue
			}
			c.HandleSet(cmd[1], strings.Join(cmd[2:], " "))
		case "del":
			containNolog := utils.ContainNolog(cmd)
			containLimit, _ := utils.ContainLimit(cmd)
			if len(cmd) < 2 {
				fmt.Println("usage: del <key> -nolog; del <startKey> <endKey> -nolog; del <lockKey> owner maxDuration lockTime -nolog")
				continue
			} else if containLimit {
				fmt.Println("usage: del <key> -nolog; del <startKey> <endKey> -nolog; del <lockKey> owner maxDuration lockTime -nolog")
				continue
			} else if len(cmd) == 2 {
				fmt.Printf("Are you sure to delete key=%s? (yes/no): \n", cmd[1])
				var confirm string
				_, err := fmt.Scan(&confirm)
				if err != nil {
					fmt.Printf("input err: %v\n", err)
					continue
				}
				if confirm != "yes" {
					continue
				} else {
					c.handleDelete(cmd[1], true)
				}
			} else if len(cmd) == 3 && containNolog {
				fmt.Printf("Are you sure to delete key=%s? (yes/no): \n", cmd[1])
				var confirm string
				_, err := fmt.Scan(&confirm)
				if err != nil {
					fmt.Printf("input err: %v\n", err)
					continue
				}
				if confirm != "yes" {
					continue
				}
				c.handleDelete(cmd[1], false)
			} else if len(cmd) == 3 && !containNolog {
				c.handleDelRange(cmd[1], cmd[2], true)
			} else if len(cmd) == 4 && containNolog {
				c.handleDelRange(cmd[1], cmd[2], false)
			} else if len(cmd) == 5 {
				lockTime, _ := strconv.Atoi(cmd[4])
				maxDuration, _ := strconv.Atoi(cmd[3])
				c.handleDeleteLock(cmd[1], cmd[2], int64(maxDuration), int64(lockTime), true)
			} else if len(cmd) == 6 && containNolog {
				lockTime, _ := strconv.Atoi(cmd[4])
				maxDuration, _ := strconv.Atoi(cmd[3])
				c.handleDeleteLock(cmd[1], cmd[2], int64(maxDuration), int64(lockTime), false)
			} else {
				fmt.Println("usage: del <key> -nolog; del <startKey> <endKey> -nolog; del <lockKey> owner maxDuration lockTime -nolog")
			}

		case "find":
			containLimit, limit := utils.ContainLimit(cmd)
			containPv := utils.ContainPv(cmd)
			containValue, value := utils.ContainValue(cmd)
			if len(cmd) < 3 {
				fmt.Println("usage: find <prefixKey> [endKey] -value=xxx -limit=n -pv")
				continue
			} else if len(cmd) == 3 && containValue {
				c.findLike(cmd[1], "", value, false, -1)
			} else if len(cmd) == 6 && containPv && containLimit && containValue {
				c.findLike(cmd[1], cmd[2], value, true, limit)
			} else if len(cmd) == 4 && containLimit && containValue {
				c.findLike(cmd[1], "", value, false, limit)
			} else if len(cmd) == 4 && containValue && !containPv {
				c.findLike(cmd[1], "", value, false, -1)
			} else if len(cmd) == 4 && containValue && containPv {
				c.findLike(cmd[1], "", value, true, -1)
			} else if len(cmd) == 5 && containLimit && containPv && containValue {
				c.findLike(cmd[1], "", value, true, limit)
			} else if len(cmd) == 5 && !containPv && containLimit && containValue {
				c.findLike(cmd[1], cmd[2], value, false, limit)
			} else if len(cmd) == 5 && !containLimit && containPv && containValue {
				c.findLike(cmd[1], cmd[2], value, true, -1)
			} else {
				fmt.Println("usage: find <prefixKey> [endKey] -value=xxx -limit=n -pv")
			}
		case "exit":
			return
		case "count":
			containValue, value := utils.ContainValue(cmd)
			if len(cmd) < 2 {
				fmt.Println("usage: count <prefixKey> [endKey]")
				continue
			} else if len(cmd) == 3 && containValue {
				c.handleCount(cmd[1], "", value)
			} else if len(cmd) == 4 && containValue {
				c.handleCount(cmd[1], cmd[2], value)
			} else if len(cmd) == 2 {
				c.handleCount(cmd[1], "", "")
			} else if len(cmd) == 3 && !containValue {
				c.handleCount(cmd[1], cmd[2], "")
			} else {
				fmt.Println("usage: count <prefixKey> [endKey]")
			}
		case "version":
			c.handleVersion()
		case "fd":
			containLimit, limit := utils.ContainLimit(cmd)
			containValue, value := utils.ContainValue(cmd)
			containNolog := utils.ContainNolog(cmd)
			if len(cmd) < 3 {
				fmt.Println("usage: fd <prefixKey> [endKey] -value=xxx -limit=n -nolog")
			} else if len(cmd) == 3 && containValue && !containLimit && !containNolog {
				c.handleFindDelete(cmd[1], "", value, -1, true)
			} else if len(cmd) == 4 && containValue && containLimit && !containNolog {
				c.handleFindDelete(cmd[1], "", value, limit, true)
			} else if len(cmd) == 4 && containValue && !containLimit && containNolog {
				c.handleFindDelete(cmd[1], "", value, -1, false)
			} else if len(cmd) == 4 && containValue && !containLimit && !containNolog {
				c.handleFindDelete(cmd[1], cmd[2], value, -1, true)
			} else if len(cmd) == 5 && containValue && containLimit && containNolog {
				c.handleFindDelete(cmd[1], "", value, limit, false)
			} else if len(cmd) == 5 && containValue && !containLimit && containNolog {
				c.handleFindDelete(cmd[1], cmd[2], value, -1, false)
			} else if len(cmd) == 5 && containValue && containLimit && !containNolog {
				c.handleFindDelete(cmd[1], cmd[2], value, limit, true)
			} else if len(cmd) == 6 && containValue && containLimit && containNolog {
				c.handleFindDelete(cmd[1], cmd[2], value, limit, false)
			} else {
				fmt.Println("usage: fd <prefixKey> [endKey] -value=xxx -limit=n -nolog")
			}
		default:
			fmt.Println("usage: get, ll, exit, set, del, find, count, version, fd")
		}
	}
}

func (c *TiKVClient) executeTxn(fn func(txn *transaction.KVTxn) error) error {
	txn, err := c.Client.Begin()
	if err != nil {
		return fmt.Errorf("transation begin err: %w", err)
	}

	if err := fn(txn); err != nil {
		_ = txn.Rollback()
		return err
	}

	if err := txn.Commit(context.Background()); err != nil {
		return fmt.Errorf("transation commit err: %w", err)
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

	if err != nil && strings.Contains(err.Error(), "not exist") {
		fmt.Printf("key:%s  not exist\n", key)
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
			fmt.Printf("iter err: %v\n", err)
			return nil
		}
		defer iter.Close()

		var count int
		for iter.Valid() {
			select {
			case <-sigCh:
				fmt.Println("\noperation cancelled")
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
				fmt.Printf("iteration failed: %v\n", err)
				break
			}

		}
		fmt.Println("-------------------")
		fmt.Printf("total: %d\n", count)
		return nil
	})
	if err != nil {
		fmt.Printf("operation failed: %v\n", err)
		return
	}
}

func (c *TiKVClient) handleListRange(key1, key2 string, pv bool, limit int) {

	// 如果key2为空，则计算key1的下一个键
	if key2 == "" {
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
			fmt.Printf("iter err: %v\n", err)
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
				fmt.Println("\noperation cancelled")
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
				fmt.Printf("iteration failed: %v\n", err)
				break
			}
		}
		fmt.Println("-------------------")
		fmt.Printf("total: %d\n", count)
		return nil
	})
	if err != nil {
		fmt.Printf("operation failed: %v\n", err)
		return
	}
}

func (c *TiKVClient) HandleSet(key, value string) {
	err := c.executeTxn(func(txn *transaction.KVTxn) error {
		return txn.Set([]byte(key), []byte(value))
	})

	if err != nil {
		fmt.Printf("operation failed: %v\n", err)
		return
	}
	fmt.Println("updated")
}

func (c *TiKVClient) handleDelete(key string, nolog bool) {
	if nolog {
		base.GlobalLogger, base.GlobalLogFile, _ = utils.InitLog()
	} else {
		base.GlobalLogFile.Close()
	}
	var result []byte
	err := c.executeTxn(func(txn *transaction.KVTxn) error {
		val, err := txn.Get(context.Background(), []byte(key))
		result = val
		return err
	})
	if err != nil {
		fmt.Println("key not exist")
		return
	}
	err = c.executeTxn(func(txn *transaction.KVTxn) error {
		return txn.Delete([]byte(key))
	})
	if err != nil {
		fmt.Printf("delete err: %v\n", err)
		return
	}
	fmt.Println("deleted")
	if base.GlobalLogger != nil {
		base.GlobalLogger.Printf("key : %s, value : %s, cmd : %s", key, string(result), cmdStr)
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
			fmt.Printf("create iteration err: %v\n", err)
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
				fmt.Println("\noperation cancelled")
				return nil
			default:
				k := iter.Key()
				v := iter.Value()
				if pv && strings.Contains(string(v), value) {
					fmt.Printf("%s", string(k))
					fmt.Printf("  Value = %s\n", string(v))
					count++
				} else {
					if strings.Contains(string(v), value) {
						fmt.Printf("%s\n", string(k))
						count++
					}
				}
			}
			if err := iter.Next(); err != nil {
				fmt.Printf("iteration failed: %v\n", err)
				break
			}
		}
		fmt.Println("-------------------")
		fmt.Printf("total: %d\n", count)
		return nil
	})
	if err != nil {
		fmt.Printf("operation failed: %v\n", err)
		return
	}
}

func (c *TiKVClient) handleDelRange(start, end string, nolog bool) {
	if nolog {
		base.GlobalLogger, base.GlobalLogFile, _ = utils.InitLog()
	} else {
		base.GlobalLogFile.Close()
	}
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

	fmt.Printf("Are you sure to delete? (yes/no): \n")
	var confirm string
	_, err := fmt.Scan(&confirm)
	if err != nil {
		fmt.Printf("input err: %v\n", err)
		return
	}
	if confirm != "yes" {
		return
	}

	for {
		txn, err := c.Client.Begin()
		if err != nil {
			fmt.Printf("transation begin err: %v\n", err)
			return
		}
		defer txn.Rollback()

		iter, err := txn.Iter(startKey, endKey)
		if err != nil {
			fmt.Printf("iter err: %v\n", err)
			return
		}
		defer iter.Close()

		processedInBatch = 0
		for iter.Valid() && processedInBatch < batchSize {
			select {
			case <-sigCh:
				fmt.Println("\noperation cancelled")
				return
			default:
				err = txn.Delete(iter.Key())
				if err != nil {
					fmt.Printf("delete key=%s err: %v\n", iter.Key(), err)
				} else {
					deletedTotal++
					processedInBatch++
					if base.GlobalLogger != nil {
						base.GlobalLogger.Printf("key : %s, value : %s, cmd : %s ", string(iter.Key()), string(iter.Value()), cmdStr)
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
				fmt.Printf("transation commit err: %v\n", err)
				return
			}
			fmt.Printf("Batch deleted: %d, Total deleted: %d\n", processedInBatch, deletedTotal)

			if iter.Valid() {
				startKey = iter.Key()
			} else {
				break
			}
		} else {
			break
		}
	}

	fmt.Println("Total deleted:", deletedTotal, "time consuming:", time.Since(startTime))
}

func (c *TiKVClient) handleDeleteLock(key, owner string, maxDuration, lockTime int64, nolog bool) {
	if nolog {
		base.GlobalLogger, base.GlobalLogFile, _ = utils.InitLog()
	} else {
		base.GlobalLogFile.Close()
	}
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh)
	deletedTotal := 0
	startTime := time.Now()

	startKey := key + "/Data/Lock"

	fmt.Printf("Are you sure to delete? (yes/no): \n")
	var confirm string
	_, err := fmt.Scan(&confirm)
	if err != nil {
		fmt.Printf("input err: %v\n", err)
		return
	}
	if confirm != "yes" {
		return
	}

	for {
		txn, err := c.Client.Begin()
		if err != nil {
			fmt.Printf("transation begin err: %v\n", err)
			return
		}
		defer txn.Rollback()

		iter, err := txn.Iter([]byte(startKey), []byte(utils.IncrementLastCharASCII(startKey)))
		if err != nil {
			fmt.Printf("iter err: %v\n", err)
			return
		}
		defer iter.Close()

		batchSize := 3000
		processedInBatch := 0
		for iter.Valid() && processedInBatch < batchSize {
			select {
			case <-sigCh:
				fmt.Println("\noperation cancelled")
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
					log.Fatalf("JSON unmarshal: %v", err)
				}

				if strings.Compare(data.Owner, owner) == 0 && data.MaxDuration == maxDuration && data.LockTime > lockTime {
					err = txn.Delete(iter.Key())
					if err != nil {
						fmt.Printf("delete key=%s err: %v\n", iter.Key(), err)
					} else {
						deletedTotal++
						processedInBatch++
						if base.GlobalLogger != nil {
							base.GlobalLogger.Printf("key : %s, value : %s, cmd : %s", string(iter.Key()), string(iter.Value()), cmdStr)
						}
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
				fmt.Printf("transation commit err: %v\n", err)
				return
			}
			fmt.Printf("Batch deleted: %d, Total deleted: %d\n", processedInBatch, deletedTotal)
		} else {
			break
		}
	}
	fmt.Println("Total deleted:", deletedTotal, "time consuming:", time.Since(startTime))
}

func (c *TiKVClient) handleCount(key1, key2, value string) {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh)
	var lastKey []byte
	var total int

	if key2 == "" {
		key2 = utils.IncrementLastCharASCII(key1)
	} else {
		key2 = utils.IncrementLastCharASCII(key2)
	}

	for {
		txn, err := c.Client.Begin()
		iter, err := txn.Iter([]byte(key1), []byte(key2))
		if err != nil {
			fmt.Printf("iter err: %v\n", err)
			return
		}
		defer iter.Close()

		var count int
		for iter.Valid() && count < 10000 {
			select {
			case <-sigCh:
				fmt.Println("\noperation cancelled")
				return
			default:
				key := iter.Key()
				if value != "" && strings.Contains(string(iter.Value()), value) {
					count++
				} else if value == "" {
					count++
				}
				lastKey = key

			}
			if err := iter.Next(); err != nil {
				fmt.Printf("iteration failed: %v\n", err)
				break
			}
		}
		//fmt.Printf("Processed %d keys in this batch\n", count)
		total += count
		if !iter.Valid() {
			fmt.Println("Total: ", total)
			return
		}

		key1 = string(append(lastKey, 0))
	}

}

func (c *TiKVClient) handleVersion() {
	fmt.Println("1.0.1")
}

func (c *TiKVClient) handleFindDelete(key1, key2, value string, limit int, nolog bool) {
	if nolog {
		base.GlobalLogger, base.GlobalLogFile, _ = utils.InitLog()
	} else {
		base.GlobalLogFile.Close()
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	if key2 == "" {
		key2 = utils.IncrementLastCharASCII(key1)
	} else {
		key2 = utils.IncrementLastCharASCII(key2)
	}

	deletedTotal := 0
	startTime := time.Now()

	fmt.Printf("Are you sure to delete? (yes/no): \n")
	var confirm string
	_, err := fmt.Scan(&confirm)
	if err != nil {
		fmt.Printf("input err: %v\n", err)
		return
	}
	if confirm != "yes" {
		return
	}

	for {
		txn, err := c.Client.Begin()
		if err != nil {
			fmt.Printf("transation begin err: %v\n", err)
			return
		}
		defer txn.Rollback()

		iter, err := txn.Iter([]byte(key1), []byte(utils.IncrementLastCharASCII(key2)))
		if err != nil {
			fmt.Printf("iter err: %v\n", err)
			return
		}
		defer iter.Close()

		batchSize := 3000
		processedInBatch := 0
		for iter.Valid() && processedInBatch < batchSize {
			if deletedTotal >= limit && limit > 0 {
				break
			}
			select {
			case <-sigCh:
				fmt.Println("\noperation cancelled")
				return
			default:
				if strings.Contains(string(iter.Value()), value) {
					err = txn.Delete(iter.Key())
					if err != nil {
						fmt.Printf("delete key=%s err: %v\n", iter.Key(), err)
					} else {
						deletedTotal++
						processedInBatch++
						if base.GlobalLogger != nil {
							base.GlobalLogger.Printf("key : %s, value : %s, cmd : %s", string(iter.Key()), string(iter.Value()), cmdStr)
						}
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
				fmt.Printf("transation commit err: %v\n", err)
				return
			}
			fmt.Printf("Batch deleted: %d, Total deleted: %d\n", processedInBatch, deletedTotal)
		} else {
			break
		}
	}

	fmt.Println("Total deleted:", deletedTotal, "time consuming:", time.Since(startTime))
}

func (c *TiKVClient) handleLog(nolog bool) {
	if nolog {
		base.GlobalLogger, base.GlobalLogFile, _ = utils.InitLog()
	} else {
		base.GlobalLogFile.Close()
	}

}
