# go-file-fetch

## 项目简介
`go-file-fetch` 是一个高效的多线程文件下载器，支持断点续传和分片下载。该项目旨在提供一个高性能、稳定可靠的文件下载解决方案，适用于大文件的快速下载。

## 功能特性
- **多线程下载**：支持多线程分片下载，显著提升下载速度。
- **断点续传**：支持从上次中断的位置继续下载，避免重复下载。
- **高效存储**：通过定时刷盘机制，确保下载进度的实时保存。
- **跨平台支持**：兼容 Windows、Linux 和 macOS。

## 使用方法

### 运行环境
- Go 版本：1.18 或更高
- 操作系统：Windows/Linux/macOS

### 下载与运行
1. 克隆项目：
   ```bash
   git clone https://github.com/your-repo/go-file-fetch.git
   cd go-file-fetch
   ```
2. 运行下载命令：
   ```bash
   go run main.go fetch <下载链接> -t <线程数>
   ```
   示例：
   ```bash
   go run main.go fetch "https://example.com/file.iso" -t 5
   ```

### 参数说明
- `<下载链接>`：目标文件的下载地址。
- `-t <线程数>`：指定下载线程数，默认为 1。

## 项目结构
```
.
├── cmd/                # 命令行入口
│   ├── fetch.go        # 下载命令实现
│   └── root.go         # 根命令
├── internal/           # 内部逻辑
│   ├── config/         # 配置管理
│   ├── downloader/     # 下载核心逻辑
│   └── ...
├── pkg/                # 公共库
│   └── log/            # 日志模块
├── main.go             # 主程序入口
├── go.mod              # Go 模块配置
└── README              # 项目说明
```

## 贡献指南
欢迎提交 Issue 和 Pull Request 来改进本项目！

## 许可证
本项目采用 [MIT 许可证](LICENSE)。