package master

import (
	"encoding/json"
	"net"
	"net/http"
	"strconv"
	"time"
	"zcron/common"
)

type ApiServer struct {
	server *http.Server
}

var (
	G_apiServer *ApiServer
)

// 保存任务接口，保存到etcdzhong
func handleSave(resp http.ResponseWriter, req *http.Request) {
	var (
		err      error
		jobParam string
		job      common.Job
		oldJob   *common.Job
		bytes    []byte
	)

	// 解析form
	if err = req.ParseForm(); err != nil {
		goto ERR
	}

	// 获取job参数
	jobParam = req.Form.Get("job")

	// 将json解析
	if err = json.Unmarshal([]byte(jobParam), &job); err != nil {
		goto ERR
	}

	// 调用jobMgr存储job内容
	if oldJob, err = G_jobMgr.SaveJob(&job); err != nil {
		goto ERR
	}

	// 返回正确内容
	if bytes, err = common.BuildResponse(0, "成功", oldJob); err == nil {
		resp.Header().Set("Content-Type", "application/json")
		resp.Write(bytes)
	}

	return
ERR:
	// 返回错误内容
	if bytes, err = common.BuildResponse(-1, err.Error(), nil); err == nil {
		resp.Header().Set("Content-Type", "application/json")
		resp.Write(bytes)
	}
}

// 初始化ApiServer
func InitApiServer() (err error) {
	var (
		mux        *http.ServeMux
		listener   net.Listener
		httpServer *http.Server
	)

	// 创建路由
	mux = http.NewServeMux()
	mux.HandleFunc("/job/save", handleSave)

	// 创建监听
	if listener, err = net.Listen("tcp", ":"+strconv.Itoa(G_config.ApiServerPort)); err != nil {
		return
	}

	// 创建http服务
	httpServer = &http.Server{
		ReadTimeout:  time.Duration(G_config.ApiReadTimeout) * time.Millisecond,
		WriteTimeout: time.Duration(G_config.ApiWriteTimeout) * time.Millisecond,
		Handler:      mux,
	}

	// 赋值单例
	G_apiServer = &ApiServer{server: httpServer}

	// 在协程中启动服务
	go httpServer.Serve(listener)

	return
}
