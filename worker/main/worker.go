package main

import (
	"flag"
	"fmt"
	"runtime"
	"time"
	"zcron/worker"
)

var (
	// configFile地址参数
	configFile string
)

func initEnv() {
	// 将go的线程数设置成当前的cpu核数，实现最高的性能
	runtime.GOMAXPROCS(runtime.NumCPU())
}

func initArgs() {
	// 设置参数解析规则
	flag.StringVar(&configFile, "config", "./worker.json", "配置文件")
	// 解析参数
	flag.Parse()
}

func main() {
	var (
		err error
	)
	initEnv()

	initArgs()

	if err = worker.InitConfig(configFile); err != nil {
		goto ERR
	}

	if err = worker.InitJobMgr(); err != nil {
		goto ERR
	}

	for {
		time.Sleep(1 * time.Second)
	}
	return
ERR:
	fmt.Println(err)
}
