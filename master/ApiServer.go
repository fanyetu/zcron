package master

import (
	"encoding/json"
	"fmt"
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

func writeJson(resp http.ResponseWriter, bytes []byte) (err error) {
	resp.Header().Set("Content-Type", "application/json")
	_, err = resp.Write(bytes)
	return
}

// 删除任务接口
func handleJobDelete(resp http.ResponseWriter, req *http.Request) {
	var (
		err     error
		jobName string
		oldJob  *common.Job
		bytes   []byte
	)
	if err = req.ParseForm(); err != nil {
		goto ERR
	}

	jobName = req.Form.Get("jobName")

	if oldJob, err = G_jobMgr.DeleteJob(jobName); err != nil {
		goto ERR
	}

	// 返回正确内容
	bytes, _ = common.BuildResponse(common.SUCCESS, "成功", oldJob)
	_ = writeJson(resp, bytes)

	return
ERR:
	fmt.Println(err)
	// 返回错误内容
	bytes, _ = common.BuildResponse(common.FAILURE, err.Error(), nil)
	_ = writeJson(resp, bytes)
}

// 保存任务接口，保存到etcdzhong
func handleJobSave(resp http.ResponseWriter, req *http.Request) {
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
	bytes, _ = common.BuildResponse(common.SUCCESS, "成功", oldJob)
	_ = writeJson(resp, bytes)

	return
ERR:
	fmt.Println(err)
	// 返回错误内容
	bytes, _ = common.BuildResponse(common.FAILURE, err.Error(), nil)
	_ = writeJson(resp, bytes)
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
	mux.HandleFunc("/job/save", handleJobSave)
	mux.HandleFunc("/job/delete", handleJobDelete)

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
