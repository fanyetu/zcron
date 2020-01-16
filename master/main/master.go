package main

import (
	"flag"
	"fmt"
	"runtime"
	"zcron/master"
)

var (
	// config地址参数
	config string
)

func initEnv() {
	// 将go的线程数设置成当前的cpu核数，实现最高的性能
	runtime.GOMAXPROCS(runtime.NumCPU())
}

func initArgs() {
	// 设置参数解析规则
	flag.StringVar(&config, "config", "./master.json", "配置文件")
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
	if err = master.InitConfig(config); err != nil {
		goto ERR
	}

	// 初始化ApiServer
	if err = master.InitApiServer(); err != nil {
		goto ERR
	}

	return
ERR:
	fmt.Println(err)
}
