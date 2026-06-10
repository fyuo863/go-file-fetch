package downloader

import (
	"go-file-fetch/internal/config"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
)

func HandleInfo() error {
	req, _ := http.NewRequest("GET", config.UI.Url, nil)
	req.Header.Set("User-Agent", config.UI.UserHeaders.UserAgent)
	req.Header.Set("Sec-CH-UA", config.UI.UserHeaders.SecChUa)
	req.Header.Set("Sec-CH-UA-Mobile", config.UI.UserHeaders.SecChUaMobile)
	req.Header.Set("Sec-CH-UA-Platform", config.UI.UserHeaders.SecChUaPlatform)
	req.Header.Set("Range", "bytes=0-0")

	resp, err := Client.HTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	getFileInfo(resp)
	getFileName(resp)
	//fmt.Println(config.UI)
	return nil
}

// 判断是否支持断点续传 (检查 206 状态码)
func getFileInfo(resp *http.Response) {
	if resp.StatusCode == http.StatusPartialContent {
		config.UI.AcceptRanges = true // 确认支持断点续传

		// 解析 Content-Range: bytes 0-0/524288000
		contentRange := resp.Header.Get("Content-Range")
		if pos := strings.LastIndex(contentRange, "/"); pos != -1 {
			sizeStr := contentRange[pos+1:]
			size, _ := strconv.ParseInt(sizeStr, 10, 64)
			config.UI.Size = size // 成功获取分块前的文件总大小
		}
	} else {
		// 核心修复：当状态码是 200 或者其他不是 206 的状态时，走这里！
		config.UI.AcceptRanges = false
		config.UI.Size = resp.ContentLength // 正常获取普通响应的文件大小
	}
}

func getFileName(resp *http.Response) {
	// 🔧 修复：trim 掉命令行残留的双引号，防止污染文件名
	config.UI.Url = strings.Trim(config.UI.Url, "\"")

	// 1. 优先尝试从 Header 获取 (最准确，因为带后缀)
	contentDisposition := resp.Header.Get("Content-Disposition")
	if contentDisposition != "" {
		parts := strings.Split(contentDisposition, "filename=")
		if len(parts) > 1 {
			name := strings.Trim(parts[1], "\"")

			// 尝试对 Header 里的名称进行 Unescape
			if decodedName, err := url.QueryUnescape(name); err == nil {
				config.UI.FileName = decodedName
			} else {
				config.UI.FileName = name
			}

			// 核心：既然从 Header 拿到了，就直接结束函数，不要让后面的 URL 逻辑覆盖它！
			if config.UI.FileName != "" {
				return
			}
		}
	}

	// 2. 如果 Header 没提供，再从 URL 路径提取
	u, err := url.Parse(config.UI.Url)
	var rawFileName string
	if err == nil {
		rawFileName = path.Base(u.Path)
	} else {
		rawFileName = path.Base(config.UI.Url)
	}

	// 将路径中的 %20 等字符转换为正常文本
	decodedName, err := url.PathUnescape(rawFileName)
	if err != nil {
		config.UI.FileName = rawFileName
	} else {
		config.UI.FileName = decodedName
	}

	// 3. 兜底保护：如果捞出来的名字为空或者是点（比如根目录），给个默认名
	if config.UI.FileName == "" || config.UI.FileName == "." {
		config.UI.FileName = "downloaded_file.bin"
	}
}
