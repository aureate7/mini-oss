# Mini OSS —— 基于 gRPC 的对象存储附件服务
Mini OSS 是一个 **基于 gRPC Streaming 的对象存储（Object Storage）原型系统**，目标是在本地/单机环境下，完整实现一个**具备工程语义的文件上传与下载服务**，重点关注：
* 大文件与高并发场景
* 流式传输与背压
* 断点续传与幂等性
* 可验证的性能与稳定性
该项目偏向 **后端系统设计 + 工程实现**，适合作为后端/系统方向的实践项目。
---
## QuickStart
本节用于 快速启动 Mini OSS，在本地完成一次 上传 + 下载 的完整流程。
### 环境要求
1. Go ≥ 1.20
2. macOS / Linux
3. 本地可用磁盘空间 ≥ 1 GB（用于压测）
### 1️⃣ 启动 Server

```bash
git clone https://github.com/aureate7/mini-oss.git
cd mini-oss

go mod tidy
go run cmd/server/mainServer.go
```

启动成功后可看到：

```text
mini-oss gRPC server started at :8080
```

------

### 2️⃣ 流式上传文件

```bash
go run cmd/client/UploadClient.go
// 需在该文件main函数中修改文件名`file`,object_id`object`,文件路径`filePath`
```

服务端文件结构：

```bash
static/
├── objects/
│   └── obj_test_001_sample_aca6ab78.zip
└── index/
    └── obj_test_001 -> ../objects/obj_test_001_sample_aca6ab78.zip
```

------

### 3️⃣ 下载文件

```bash
go run cmd/client/download.go 
// 需在该文件main函数中修改要下载的文件`objectID`、名称`outName`、以及输出目录`outDirRel`
```

支持断点续传：上传/下载中途取消后再重新上传/下载，可以延续上一次操作。

------

### 4️⃣ 压测

#### 上传压测

```bash
go run cmd/bench/upload_bench.go \
  --addr 127.0.0.1:8080 \
  --concurrency 100 \
  --file_size 256MiB
// or 在文件main函数中修改要上传的文件位置`filePath`、并发量`con`、块大小`chunk`
```

#### 下载压测

```bash
go run cmd/bench/download_bench.go \
  --addr 127.0.0.1:8080 \
  --concurrency 100 \
  --object_id obj_test_001
// or 在文件main函数中修改并发量`con`、要下载文件的index（object_id）`object`
```

---
## 项目目标
### 功能目标
* 提供 **对象上传、下载、查询** 等基础能力
* 支持 **客户端流式上传（Client Streaming）**
* 支持 **服务端流式下载（Server Streaming）**
* 支持 **Range 下载 / 断点续传**
* 支持 **数据完整性校验（CRC32 + SHA256）**
### 非功能目标
* **高并发**：支持 ≥100 并发上传 / 下载
* **高吞吐**：在本地高速环境下尽可能榨干 IO / 带宽
* **稳定性**：支持异常中断后的恢复（Resume）
* **工程可扩展性**：模块清晰，可平滑扩展到分布式架构
* **可观测性**：输出基础吞吐与延迟指标，支持 benchmark
---
## 系统设计概览
### Upload Session 机制（核心）
* 上传前需通过 `CreateUploadSession` 创建上传会话
* Session 记录：

    * `upload_id`
    * `object_id`
    * `file_size`
    * `chunk_size`
    * `uploaded_offset`
    * `sha256`
* 支持：
    * **断点续传**（基于 uploaded_offset）
    * **幂等重试**（重复 chunk 自动忽略）
    * **顺序约束**（防止乱序 / 丢段）
---
### 流式上传（Client Streaming Upload）
* 客户端按 chunk 分片发送数据
* 服务端逐 chunk 校验并落盘：
    * offset 连续性校验
    * CRC32 校验（可选）
    * 顺序写入（WriteAt）
* 上传完成后进行 **全文件 SHA256 校验**
---
### 流式下载（Server Streaming Download）
* 基于 `io.SectionReader` 顺序读取
* 支持：
    * 指定 `start_offset / end_offset`
    * 客户端断点续传
* 顺序读 + Streaming 发送，减少 syscall 次数，提高吞吐
---
### 原子提交与索引解耦
* 上传阶段使用临时文件：
  ```
  <object_id>_<filename>.part.<upload_id>
  ```
* 完成后原子 `rename` 为最终文件：
  ```
  <object_id>_<filename>_<upload_id>.<ext>
  ```
* 通过索引目录建立访问入口：
  ```
  static/index/<object_id> -> ../objects/<final_file>
  ```
* **Upload / Download 解耦**
* Download 始终通过 `object_id` 访问，避免真实文件名变化带来的影响
---
## 项目目录结构
```
mini-oss/
├── service_proto/
│   ├── proto/oss/v1/        # gRPC proto 定义
│   └── pb/oss/v1/           # 生成代码
├── cmd/
│   ├── server/              # gRPC Server
│   │   └── main.go
│   ├── client/              # 功能性客户端
│   └── bench/               # 上传 / 下载压测工具
│       ├── upload_bench.go
│       └── download_bench.go
├── internal/
│   ├── store/               # Session / 元信息存储（内存实现，可扩展）
│   ├── auth/                # 预留：鉴权拦截器
│   ├── limiter/             # 预留：限流
│   └── logging/             # 预留：日志与观测
└── static/
    ├── objects/             # 实际文件存储
    └── index/               # object_id 索引入口
```