package log

import (
	"os"
	"time"

	"charm.land/log/v2"
)

var Logger *log.Logger

func LogSetting() {
	Logger = log.New(os.Stderr) //DateTime
	Logger.SetTimeFormat(time.DateTime)
	defer Logger.Info("成功加载charm.log", "butter", true)
	//Logger.Info("chewy!", "butter", true)
}
