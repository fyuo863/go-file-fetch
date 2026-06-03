package log

import (
	"os"
	"time"

	"charm.land/log/v2"
)

var Logger *log.Logger

func LogSetting() {
	Logger = log.New(os.Stderr)
	// 💡 修复：将 FatalLevel 改为 InfoLevel，这样你才能看到断点续传的提示
	Logger.SetLevel(log.InfoLevel) 
	Logger.SetTimeFormat(time.DateTime)
	defer Logger.Info("成功加载charm.log", "butter", true)
}
