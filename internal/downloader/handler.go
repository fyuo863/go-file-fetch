package downloader

import (
	"net/http"
)

var Client *MyClient

type MyClient struct {
	HTTPClient *http.Client
}

func HandlerInit() {
	// 1. 创建自定义客户端
	Client = &MyClient{HTTPClient: &http.Client{}}
}
