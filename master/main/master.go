package main

import (
	"flag"
	"fmt"
	"runtime"
	"time"
	"zcron/master"
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
	flag.StringVar(&configFile, "config", "./master.json", "配置文件")
	// 解析参数
	flag.Parse()
}

func main() {
	var (
		err error
	)
	// 初始化命令行参数
	initArgs()

	// 初始化线程
	initEnv()

	// 初始化配置
	if err = master.InitConfig(configFile); err != nil {
		goto ERR
	}

	// 初始化任务管理器
	if err = master.InitJobMgr(); err != nil {
		goto ERR
	}

	// 初始化ApiServer
	if err = master.InitApiServer(); err != nil {
		goto ERR
	}

	for {
		time.Sleep(1 * time.Second)
	}

	return
ERR:
	fmt.Println(err)
}
