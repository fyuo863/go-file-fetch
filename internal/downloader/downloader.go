package downloader

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"go-file-fetch/internal/config"
	"go-file-fetch/pkg/log"
	"io"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"time"
)

var Wg sync.WaitGroup

// ==========================================
// 💡 核心改良：解耦「控制体」与「纯数据体」
// ==========================================

// ChunkProgress 记录单个线程分片的下载状态
type ChunkProgress struct {
	Index   int   `json:"index"`   // 切片索引 (从 0 开始)
	Start   int64 `json:"start"`   // 该切片的绝对起始物理字节位置
	End     int64 `json:"end"`     // 该切片的绝对结束物理字节位置
	Current int64 `json:"current"` // 该切片当前已经下载到的绝对位置 (断点续传从此开始)
}

// MetaFooter 纯粹的数据结构，内部不包含任何锁，专门用来序列化为干净的 JSON
type MetaFooter struct {
	Url       string          `json:"url"`        // 下载源地址 (校验是否是同一个文件)
	TotalSize int64           `json:"total_size"` // 文件的真实总大小
	Chunks    []ChunkProgress `json:"chunks"`     // 所有切片的进度明细
}

// SafeMetaFooter 线程安全的内存进度追踪器 (负责拦截多线程高并发累加)
type SafeMetaFooter struct {
	mu        sync.RWMutex // 保护并发安全的读写锁，防止多线程竞争
	Url       string
	TotalSize int64
	Chunks    []ChunkProgress
}

// UpdateProgress 内存原子操作：每下载一次字节就累加一次对应的 Current，绝不引发写盘卡顿
func (sm *SafeMetaFooter) UpdateProgress(chunkIndex int, n int) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.Chunks[chunkIndex].Current += int64(n)
}

// Clone 核心修复：让 Clone 返回不带锁的纯数据 MetaFooter，彻底根除 json.Marshal 报警
func (sm *SafeMetaFooter) Clone() MetaFooter {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	// 必须执行切片深拷贝，避免外部并发迭代导致 panic
	chunksCopy := make([]ChunkProgress, len(sm.Chunks))
	copy(chunksCopy, sm.Chunks)

	return MetaFooter{
		Url:       sm.Url,
		TotalSize: sm.TotalSize,
		Chunks:    chunksCopy,
	}
}

// ==========================================
// 💡 物理文件控制块（Footer 4KB）落盘与加载
// ==========================================

// FlushMetaToFooter 将内存最新的进度快照，强行安全覆盖写入文件最后的 4KB 空间
func FlushMetaToFooter(file *os.File, realSize int64, sm *SafeMetaFooter) error {
	snapshot := sm.Clone() // 👈 此时 snapshot 是无锁的 MetaFooter，警告解除！

	jsonData, err := json.Marshal(snapshot)
	if err != nil {
		return err
	}

	// 预留了 4096 字节，扣除 4 字节长度标头，JSON 理论上限不能超过 4092 字节
	if len(jsonData) > 4092 {
		return fmt.Errorf("元数据过大，超出了预分配的 4KB 空间")
	}

	// 构造固定 4KB 的本地缓冲区
	buf := make([]byte, 4096)
	copy(buf, jsonData) // 将 JSON 写入缓冲区开头

	// 在缓冲区的最后 4 字节，用大端序写入 JSON 的真实长度
	jsonLen := uint32(len(jsonData))
	binary.BigEndian.PutUint32(buf[4092:], jsonLen)

	// 原子覆盖物理写入：整块 4KB 覆盖写入到真实大小（realSize）之后的绝对偏移位置
	_, err = file.WriteAt(buf, realSize)
	return err
}

// ReadMetaFromFooter 从已有的文件末尾尝试反序列化出历史下载进度
func ReadMetaFromFooter(file *os.File) (*SafeMetaFooter, error) {
	log.Logger.Info("Downloader", "ReadMetaFromFooter")
	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}
	fileLen := stat.Size()
	if fileLen < 4 {
		return nil, fmt.Errorf("文件太小，无有效控制块")
	}

	// 1. 读出最后 4 字节的长度标头
	lenBuf := make([]byte, 4)
	if _, err := file.ReadAt(lenBuf, fileLen-4); err != nil {
		return nil, err
	}
	jsonLen := int64(binary.BigEndian.Uint32(lenBuf))
	log.Logger.Info("Downloader", "读取控制块长度, jsonLen", jsonLen)
	if jsonLen <= 0 || jsonLen > fileLen-4 || jsonLen > 4092 {
		return nil, fmt.Errorf("非法的控制块长度")
	}

	// 2. 读出前面的 JSON 字节
	jsonOffset := fileLen - 4096
	jsonData := make([]byte, jsonLen)
	if _, err := file.ReadAt(jsonData, jsonOffset); err != nil {
		return nil, err
	}

	// 3. 核心修复：先解析为纯数据体 MetaFooter，再组装回带锁的控制体指针
	var mf MetaFooter
	if err := json.Unmarshal(jsonData, &mf); err != nil {
		return nil, err
	}

	log.Logger.Info("Downloader", "chunks", mf.Chunks)

	return &SafeMetaFooter{
		Url:       mf.Url,
		TotalSize: mf.TotalSize,
		Chunks:    mf.Chunks,
	}, nil
}

// ==========================================
// 🚀 核心调度器逻辑（主线程）
// ==========================================

func SetupDownload(threads int) {
	HandlerInit()
	err := HandleInfo() // 获取文件信息
	if err != nil {
		log.Logger.Error("Downloader", "获取文件信息失败", err)
		return
	}
	DownloadManager(threads)
}

func DownloadManager(threads int) {
	// 用 OpenFile 打开，若文件已存在则不破坏它，供断点续传恢复
	tmpFileName, f, err := NewFile()
	if err != nil {
		log.Logger.Error("Downloader", "创建或打开文件占位失败,error", err)
		return
	}

	var hasError bool // 状态追踪：标记整个下载期是否遇到致命异常

	// 无论发生什么，最后都由主线程来统一安全关闭和重命名
	// 传递 hasError 状态，用来在最后一秒决定是“截断去尾”还是“保留断点”
	defer func() {
		err := FileRename(f, tmpFileName, hasError)
		if err != nil {
			log.Logger.Error("Downloader", "文件重命名失败", err)
		}
	}()

	threadCount := 1
	if config.UI.AcceptRanges && config.UI.Size > 0 {
		threadCount = threads
	}

	errCh := make(chan error, threadCount)

	// 初始化并发安全的内存进度记录器
	globalMeta := &SafeMetaFooter{
		Url:       config.UI.Url,
		TotalSize: config.UI.Size,
		Chunks:    []ChunkProgress{},
	}

	// 尝试从历史控制块中读取历史进度
	historyMeta, err := ReadMetaFromFooter(f)

	if err == nil && len(historyMeta.Chunks) == threadCount {
		log.Logger.Info("Downloader", "进度记录", true)
		globalMeta.Chunks = historyMeta.Chunks
	} else {
		log.Logger.Info("Downloader", "进度记录", false)
		// 如果是全新的任务，且支持切片，进行第一次切片区间划分
		if config.UI.AcceptRanges && config.UI.Size > 0 {
			for i := 0; i < threadCount; i++ {
				start := i * int(config.UI.Size) / threadCount
				end := start + int(config.UI.Size)/threadCount - 1
				if i == threadCount-1 {
					end = int(config.UI.Size) - 1
				}
				globalMeta.Chunks = append(globalMeta.Chunks, ChunkProgress{
					Index:   i,
					Start:   int64(start),
					End:     int64(end),
					Current: int64(start), // 初始当前位置等于区间起点
				})
			}
		}
	}

	// 启动异步定时刷盘守护协程
	ctx, cancelCtx := context.WithCancel(context.Background())
	defer cancelCtx()
	if config.UI.AcceptRanges && config.UI.Size > 0 {
		go func(ctx context.Context, file *os.File, realSize int64, sm *SafeMetaFooter) {
			ticker := time.NewTicker(200 * time.Millisecond) // 每 200 毫秒高频刷新一次文件尾
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					_ = FlushMetaToFooter(file, realSize, sm)
				case <-ctx.Done():
					// 临退场前，发起最后一次强制冲刷，确保进度 100% 精准
					_ = FlushMetaToFooter(file, realSize, sm)
					return
				}
			}
		}(ctx, f, config.UI.Size, globalMeta)
	}

	// 分流：多线程切片 vs 单线程全量流式
	if config.UI.AcceptRanges && config.UI.Size > 0 {
		log.Logger.Info("Downloader", "AcceptRanges", config.UI.AcceptRanges, "threads", threads)
		for i := 0; i < threadCount; i++ {
			chunk := globalMeta.Chunks[i]

			// 如果该切片之前在历史记录中已经全下完了，直接跳过它
			if chunk.Current > chunk.End {
				continue
			}

			Wg.Add(1)
			go func(c ChunkProgress) {
				defer Wg.Done()
				// 将续传指针 c.Current 作为请求开头传给 Worker
				Worker(errCh, f, c.Current, c.End, true, globalMeta, c.Index)
			}(chunk)
		}
	} else {
		log.Logger.Info("Downloader", "AcceptRanges", config.UI.AcceptRanges, "threads", 1)
		Wg.Add(1)
		go func() {
			defer Wg.Done()
			// 非 Range 链接（如 GitHub Chunked 传输），globalMeta 传 nil
			Worker(errCh, f, 0, -1, false, nil, 0)
		}()
	}

	go func() {
		Wg.Wait()
		close(errCh)
	}()

	// 主线程常驻接收错误
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)

logLoop:
	for {
		select {
		case err, ok := <-errCh:
			if !ok {
				// 当所有 Worker 正常结束且 errCh 被关闭时，退出整个大循环
				break logLoop
			}
			if err != nil {
				log.Logger.Error("Downloader", "下载过程中收到错误", "error", err)
				hasError = true
			}
		case <-sigCh:
			// 🎯 核心：当用户按下 Ctrl+C 时，会瞬间击中这里
			log.Logger.Warn("Downloader", "Ctrl+C", true)
			hasError = true // 强制标记为错误，通知 FileRename 保持 .tmp 后缀不被削尾

			// 强行把当前内存里的最新进度往文件末尾刷一次盘
			if config.UI.Size > 0 {
				_ = FlushMetaToFooter(f, config.UI.Size, globalMeta)
			}

			// 善后：触发 defer 中的 FileRename（关闭文件指针），然后安全闪人
			return
		}
	}

	// 全线收工，注销定时刷盘器
	cancelCtx()

	// 传递成功/失败标志给尾部报告处理
	End(config.UI.FileName, hasError)
}

// ==========================================
// 🛠️ 底层跨平台系统逻辑实现
// ==========================================

func NewFile() (string, *os.File, error) {
	tmpFileName := config.UI.FileName + ".tmp"

	// 允许读写，文件存在时不截断它（不使用 os.Create，防止清除未下载完的文件）
	f, err := os.OpenFile(tmpFileName, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return tmpFileName, nil, err
	}

	if config.UI.AcceptRanges && config.UI.Size > 0 {
		// 全平台通用物理预占位：真实文件大小 + 4KB 缓冲控制尾巴
		totalSize := config.UI.Size + 4096

		// 直接裁剪锁定目标大小，规避文件空洞与下载到一半报磁盘满的 Bug
		if err := f.Truncate(totalSize); err != nil {
			f.Close()
			return tmpFileName, nil, err
		}
	}

	return tmpFileName, f, nil
}

func FileRename(f *os.File, tmpFileName string, hasError bool) error {
	// 如果完整下载，没有抛出错误，启动 Truncate 系统调用，0 毫秒瞬间削掉 4KB 尾巴！
	if !hasError && config.UI.Size > 0 {
		if err := f.Truncate(config.UI.Size); err != nil {
			log.Logger.Error("Downloader", "截断文件移除暂存数据失败", "error", err)
		}
	}

	if err := f.Close(); err != nil {
		log.Logger.Error("Downloader", "关闭文件失败", "error", err)
	} else {
		// 如果无错，剥离 .tmp 后缀恢复正常文件名
		if !hasError {
			if err := os.Rename(tmpFileName, config.UI.FileName); err != nil {
				log.Logger.Error("Downloader", "重命名文件失败", "error", err)
			}
		} else {
			// 如果有错，保留带有 4KB 记录的 .tmp 文件，方便用户重启程序断点续传
			log.Logger.Warn("Downloader", "任务未完成，已在文件尾部暂存断点，下次启动可自动续传,file", tmpFileName)
		}
	}
	return nil
}

func Worker(errCh chan<- error, file *os.File, start, end int64, useRange bool, globalMeta *SafeMetaFooter, chunkIndex int) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, config.UI.Url, nil)
	if err != nil {
		errCh <- err
		return
	}

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

	if resp.StatusCode != http.StatusPartialContent && resp.StatusCode != http.StatusOK {
		errCh <- fmt.Errorf("下载失败，状态码: %s", resp.Status)
		return
	}

	buf := make([]byte, 32*1024)
	offset := start
	for {
		n, readErr := resp.Body.Read(buf)
		if n > 0 {
			if _, writeErr := file.WriteAt(buf[:n], offset); writeErr != nil {
				errCh <- fmt.Errorf("写入失败: %w", writeErr)
				return
			}
			offset += int64(n)

			// 👈 每次成功写入本地文件，通知内存进行轻量级无锁原子相加
			if globalMeta != nil {
				globalMeta.UpdateProgress(chunkIndex, n)
			}
		}
		if readErr != nil {
			if readErr != io.EOF {
				errCh <- fmt.Errorf("读取失败: %w", readErr)
			}
			break
		}
	}
}

func End(fileName string, hasError bool) {
	if hasError {
		return
	}
	log.Logger.Info("End", "下载完成", fileName)
}
