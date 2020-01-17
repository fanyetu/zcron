package master

import (
	"encoding/json"
	"io/ioutil"
)

type Config struct {
	ApiServerPort   int      `json:"apiServerPort"`
	ApiReadTimeout  int      `json:"apiReadTimeout"`
	ApiWriteTimeout int      `json:"apiWriteTimeout"`
	EtcdEndpoints   []string `json:"etcdEndpoints"`
	EtcdDialTimeout int      `json:"etcdDialTimeout"`
	StaticDir       string   `json:"staticDir"`
}

var (
	G_config *Config
)

// 初始化配置文件
func InitConfig(filename string) (err error) {
	var (
		content []byte
		config  Config
	)
	// 读取配置文件
	if content, err = ioutil.ReadFile(filename); err != nil {
		return
	}

	// 将配置文件内容反解
	if err = json.Unmarshal(content, &config); err != nil {
		return
	}

	// 设置单例
	G_config = &config
	return
}
