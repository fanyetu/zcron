package worker

import (
	"encoding/json"
	"io/ioutil"
)

// Config 配置
type Config struct {
	EtcdEndpoints   []string `json:"etcdEndpoints"`
	EtcdDialTimeout int      `json:"etcdDialTimeout"`
	MongoUrl        string   `json:"mongoUrl"`
	MongoTimeout    int      `json:"mongoTimeout"`
	BatchSize       int      `json:"batchSize"`
	BatchTimeout    int      `json:"batchTimeout"`
}

var (
	G_config *Config
)

// InitConfig 初始化配置文件
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
