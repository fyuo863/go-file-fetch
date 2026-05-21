package config

type UrlInfo struct {
	Url          string
	UserAgent    string
	FileName     string
	Size         int64
	AcceptRanges bool
	//tracker      *monitor.ProgressTracker
}

// 💡 关键：定义一个外部可以访问的全局变量指针
var UI *UrlInfo

func ConfigInit(url string, threads int) {
	// 实例化结构体，并把传入的 url 存进去
	UI = &UrlInfo{
		Url:       url, // 记得把接收到的 url 参数存进来！
		UserAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
	}
	//log.Logger.Info("Config", "初始化成功", true)
	//fmt.Println("Config 初始化成功:", UI)
}
