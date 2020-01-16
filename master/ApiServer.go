package master

import (
	"net"
	"net/http"
	"strconv"
	"time"
)

type ApiServer struct {
	server *http.Server
}

var (
	G_apiServer *ApiServer
)

func handleSave(resp http.ResponseWriter, req *http.Request) {

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
