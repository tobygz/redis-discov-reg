package main

import (
	"context"
	"fmt"
	redis "github.com/redis/go-redis/v9"
	"math/rand"
	"strings"
	"time"
)

const prefix_addr = "sb_discov__"

func monitor(rdb *redis.Client, chWait chan int, myname string) {
	//config set notify-keyspace-events KE
	rdb.ConfigSet(context.Background(), "notify-keyspace-events", "KE$sxem")

	subval := fmt.Sprintf("__keyspace@0__:%s*", prefix_addr)
	psub := rdb.PSubscribe(context.Background(), subval)
	go func() {
		for {
			select {
			case <-chWait:
				return
			default:
				message, err := psub.ReceiveMessage(context.Background())
				if err != nil {
					panic(err)
					return
				}
				ary := strings.Split(message.Channel, ":")
				keyval := ary[1]
				result, err := rdb.Get(context.Background(), keyval).Result()
				if err != nil {
					panic(err)
					return
				}
				//jss, _ := json.Marshal(message)
				//fmt.Printf("[%s] pubsub recv : %s\r\n", myname, string(jss))
				//get key from channel

				fmt.Printf("[%s] get set, key: %s val: %s\r\n", myname, keyval, result)
			}
		}
	}()
}

func doRegDiscov(port string, ip string, tpe string, name string) {
	time.Sleep(time.Second * time.Duration(rand.Intn(10)))
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379", // use default Addr
		Password: "",               // no password set
		DB:       0,                // use default DB
	})

	pong, err := rdb.Ping(context.Background()).Result()
	fmt.Println(pong, err)

	//generate addr content
	content := fmt.Sprintf("%s_%s_%s_%s", ip, port, tpe, name)
	//reg

	chWait := make(chan int, 2)
	monitor(rdb, chWait, name)
	go func() {
		rdb.Set(context.Background(), prefix_addr+"_"+name, content, time.Second*10)
		for {
			select {
			case <-chWait:
				rdb.Del(context.Background(), prefix_addr+"_"+name)
			default:
				rdb.Expire(context.Background(), prefix_addr+"_"+name, time.Second*10)
				time.Sleep(time.Second * 7)
			}
		}
	}()

	//get all
	var cursor uint64
	var keys []string
	keys, cursor, err = rdb.Scan(context.Background(), cursor, prefix_addr+"*", 10240).Result()
	for _, key := range keys {
		fmt.Println(name, " get all key:", key)
	}

	time.Sleep(time.Minute)
	chWait <- 0
	chWait <- 0
	fmt.Println("name:", name, " exited")
}

func main() {
	//port string, ip string, tpe string, name string
	go doRegDiscov("8888", "127.0.0.1", "roommgr", "roommgr_001")
	go doRegDiscov("8887", "127.0.0.1", "roommgr", "roommgr_002")
	go doRegDiscov("8886", "127.0.0.1", "roommgr", "roommgr_003")
	go doRegDiscov("8885", "127.0.0.1", "roommgr", "roommgr_004")
	go doRegDiscov("8884", "127.0.0.1", "roommgr", "roommgr_005")
	fmt.Println("haha")
	ch := make(chan int, 0)
	<-ch
}
