package downloader

import (
	"context"
	"fmt"
	"go-file-fetch/internal/config"
	"go-file-fetch/pkg/log"
	"io"
	"net/http"
	"os"
	"sync"
)

var Wg sync.WaitGroup

// SetupDownload 负责接收解析好的参数并执行下载
func SetupDownload(threads int) {
	//log.Logger.Info("Downloader", "核心逻辑收到的线程数:", threads)
	HandlerInit()
	HandleInfo() // 获取文件信息
	DownloadManager(threads)
}

func DownloadManager(threads int) {
	tmpFileName, f, err := NewFile() // 创建文件占位
	if err != nil {
		log.Logger.Error("Downloader", "创建文件占位失败", "error", err)
		return
	}
	// 确保无论发生什么，最后都由主线程来统一安全关闭和重命名
	defer FileRename(f, tmpFileName)

	threadCount := 1
	if config.UI.AcceptRanges && config.UI.Size > 0 {
		threadCount = threads
	}

	errCh := make(chan error, threadCount)

	// --- 核心修复：根据是否支持断点续传分流处理 ---
	if config.UI.AcceptRanges && config.UI.Size > 0 {
		// A 分支：支持断点续传，走切块多线程下载
		log.Logger.Info("Downloader", "AcceptRanges", config.UI.AcceptRanges, "threads", threads)
		for i := 0; i < threadCount; i++ {
			start := i * int(config.UI.Size) / threadCount
			end := start + int(config.UI.Size)/threadCount - 1
			if i == threadCount-1 {
				end = int(config.UI.Size) - 1
			}
			Wg.Add(1)
			go func(start, end int) {
				defer Wg.Done()
				Worker(errCh, f, start, end, true) // true 代表需要带 Range
			}(start, end)
		}
	} else {
		// B 分支：流式传输（如GitHub的-1）或不支持Range，强制单线程从头下到尾
		log.Logger.Info("Downloader", "AcceptRanges", config.UI.AcceptRanges, "threads", 1)
		Wg.Add(1)
		go func() {
			defer Wg.Done()
			Worker(errCh, f, 0, -1, false) // false 代表不需要带 Range 响应头
		}()
	}

	// --- 核心修复：优雅接收错误，避免死锁 ---
	// 启动一个独立的 goroutine 负责在所有 worker 结束后安全关闭 channel
	go func() {
		Wg.Wait()
		close(errCh)
	}()

	// 主线程在这里安全地消费错误
	for err := range errCh {
		if err != nil {
			log.Logger.Error("Downloader", "下载过程中收到错误", "error", err)
		}
	}
	End()
}

func NewFile() (string, *os.File, error) {
	tmpFileName := config.UI.FileName + ".tmp"
	if config.UI.FileName == "" {
		tmpFileName = "file_is_downloading.tmp"
	}
	f, err := os.Create(tmpFileName)
	if err != nil {
		return tmpFileName, nil, err
	}
	// 💡 修复：这里绝对不能写 defer f.Close()！
	return tmpFileName, f, nil
}

func FileRename(f *os.File, tmpFileName string) error {
	// Wg.Wait() 已经在外层或本处前置确保完成
	if err := f.Close(); err != nil {
		log.Logger.Error("Downloader", "关闭文件失败", "error", err)
	} else {
		// 只有关闭成功了，再去重命名
		if err := os.Rename(tmpFileName, config.UI.FileName); err != nil {
			log.Logger.Error("Downloader", "重命名文件失败", "error", err)
		}
	}
	return nil
}

// Worker 下载指定范围的文件分片，并写入文件
func Worker(errCh chan<- error, file *os.File, start, end int, useRange bool) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, config.UI.Url, nil)
	if err != nil {
		errCh <- err
		return
	}

	// 💡 修复：如果是流式下载，不加 Range 头
	if useRange {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))
	}
	req.Header.Set("User-Agent", config.UI.UserAgent)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		select {
		case <-ctx.Done():
			return
		default:
			errCh <- err
			return
		}
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	// 💡 修复：普通单线程下载状态码是 200
	if resp.StatusCode != http.StatusPartialContent && resp.StatusCode != http.StatusOK {
		errCh <- fmt.Errorf("下载失败，状态码: %s", resp.Status)
		return
	}

	buf := make([]byte, 32*1024)
	offset := int64(start)
	for {
		n, readErr := resp.Body.Read(buf)
		if n > 0 {
			if _, writeErr := file.WriteAt(buf[:n], offset); writeErr != nil {
				errCh <- fmt.Errorf("写入失败: %w", writeErr)
				return
			}
			offset += int64(n)
		}
		if readErr != nil {
			if readErr != io.EOF {
				errCh <- fmt.Errorf("读取失败: %w", readErr)
			}
			break
		}
	}
}

func End() {
	log.Logger.Info("TODO", "下载失败后删除或暂存")
}
