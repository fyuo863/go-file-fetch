package downloader

import (
	"net/http"
	"time"
)

var Client *MyClient

type MyClient struct {
	HTTPClient *http.Client
}

func HandlerInit() {
	// 💡 修复：配置连接超时和响应头超时，防止单个线程卡死
	Client = &MyClient{
		HTTPClient: &http.Client{
			Transport: &http.Transport{
				Proxy:                 http.ProxyFromEnvironment,
				ResponseHeaderTimeout: 15 * time.Second, // 等待服务器响应头的最大时间
				IdleConnTimeout:       30 * time.Second, // 空闲连接保活时间
			},
			// Timeout: 0, 这里不设置整体 Timeout，因为大文件下载可能需要很久，我们靠 Transport 限制
		},
	}
}