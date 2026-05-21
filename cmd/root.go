package cmd

import (
	"go-file-fetch/pkg/log"
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "go-file-fetch",
	Short: "go-file-fetch 是一个高效的命令行下载工具",
	Long:  `这是一个使用 Go 语言和 Cobra 框架构建的多线程并发下载器。`,
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		log.Logger.Error("未找到命令树", "error", err)
		//fmt.Println(err)
		os.Exit(1)
	}
}
