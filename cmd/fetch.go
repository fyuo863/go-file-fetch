package cmd

import (
	"fmt"
	"go-file-fetch/internal/config"
	"go-file-fetch/internal/downloader"
	"go-file-fetch/pkg/log"
	"strings"

	"github.com/spf13/cobra"
)

var (
	ThreadCount int
	DownloadDir string
)

func init() {
	// 将 fetchCmd 添加到根命令中
	rootCmd.AddCommand(fetchCmd)
	log.LogSetting()
	// 添加一个可选的 flag 参数：-t 或 --threads，默认 4 线程
	fetchCmd.Flags().IntVarP(&ThreadCount, "threads", "t", 4, "并发下载的线程数")
	// 添加一个可选的 flag 参数：-d 或 --dir，指定下载目录，默认当前目录
	fetchCmd.Flags().StringVarP(&DownloadDir, "dir", "d", ".", "下载保存目录")
}

var fetchCmd = &cobra.Command{
	Use:   "fetch [url]",
	Short: "通过 URL 下载文件",
	Args:  cobra.ExactArgs(1), // 严格限制必须且只能传入 1 个参数 (URL)
	Run: func(cmd *cobra.Command, args []string) {
		targetURL := args[0]

		// 🔧 修复：强力清除命令行残留的引号和空白字符
		targetURL = strings.Trim(targetURL, "\"' ")
		targetURL = strings.TrimSpace(targetURL)

		fmt.Printf("[DEBUG] 收到的 URL: %q\n", targetURL)

		// 调用 downloader 包的函数，把参数传过去
		config.ConfigInit(targetURL, ThreadCount, DownloadDir)
		downloader.SetupDownload(ThreadCount)
	},
}
