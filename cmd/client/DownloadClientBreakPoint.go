package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	ossv1 "github.com/aureate7/mini-oss/service_proto/pb/oss/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// 本版本含断点续传功能
// 从服务器中下载文件 Download
func main() {
	addr := "127.0.0.1:8080"

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("failed to create grpc client: %v", err)
	}
	defer conn.Close()

	client := ossv1.NewDownloadServiceClient(conn)

	// 给大文件续传留足时间；生产可把超时做成可配置
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// 用户请求常量设定
	const (
		objectID  = "gcc-master.zip"
		hasEnd    = false
		end       = uint64(0)  // hasEnd=false 时忽略
		chunkSize = 256 * 1024 // 客户端期望 chunk 大小（服务端可 clamp）
		outName   = "gcc-master.zip"
		outDirRel = "static/downloads"
	)

	outDir := filepath.Clean(outDirRel) // 清除输出目录路径的多余字符
	if err := os.MkdirAll(outDir, 0755); err != nil {
		log.Fatalf("could not create out dir: %v", err)
	}
	outPath := filepath.Join(outDir, outName) // 获取最终输出路径

	// 断点续传：不要用 O_TRUNC(若文件存在，则会先清空里面内容再写入新内容)；需要读写权限以支持 WriteAt/Stat
	file, err := os.OpenFile(outPath, os.O_CREATE|os.O_RDWR, 0600) // 打开目标文件
	if err != nil {
		log.Fatalf("could not open file: %v", err)
	}
	defer file.Close()

	// ===== 断点：以本地文件大小作为 startOffset =====
	st, err := file.Stat() // 获取目标文件元数据meta-data（即右键后的属性信息）
	if err != nil {
		log.Fatalf("stat failed: %v", err)
	}

	var start uint64
	if st.Size() > 0 {
		start = uint64(st.Size()) // 已有文件（之前下载过的一半）
	} else {
		start = 0 // 空文件很正常，代表从头开始
	}

	// 若用户指定了 range 右边界，且本地已达到/超过 end，则直接结束
	if hasEnd && start >= end {
		log.Printf("already downloaded: object_id=%s start=%d end=%d out=%s", objectID, start, end, outPath)
		return
	}

	log.Printf("resume download: object_id=%s start_offset=%d has_end=%t end=%d out=%s", objectID, start, hasEnd, end, outPath)

	// ===== 发起下载（从 startOffset 继续） =====
	stream, err := client.Download(ctx, &ossv1.DownloadRequest{
		ObjectId:           objectID,
		StartOffset:        start,
		HasEnd:             hasEnd,
		EndOffset:          end,
		PreferredChunkSize: chunkSize,
	})
	if err != nil {
		log.Fatalf("could not start download: %v", err)
	}

	var (
		index     int
		total     uint64
		expectOff = start // 连续性校验：当前服务端按顺序发送
	)

	for {
		res, rerr := stream.Recv()
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			if st, ok := status.FromError(rerr); ok && st.Code() == codes.DeadlineExceeded {
				log.Fatalf("download timeout: %v", rerr)
			}
			log.Fatalf("could not receive: %v", rerr)
		}
		index++

		// 顺序/断点一致性校验：帮助快速发现服务端 offset 逻辑问题
		if res.GetOffset() != expectOff {
			log.Fatalf("unexpected offset: expect=%d got=%d", expectOff, res.GetOffset())
		}

		payload := res.GetPayload()
		if len(payload) == 0 {
			log.Fatalf("empty payload at offset %d", res.GetOffset())
		}

		// 按 offset 写入：支持重试/断点（比 bufio.Writer 更契合）
		n, werr := file.WriteAt(payload, int64(res.GetOffset()))
		if werr != nil {
			log.Fatalf("could not write: %v", werr)
		}
		if n != len(payload) {
			log.Fatalf("partial write: wrote=%d want=%d", n, len(payload))
		}

		total += uint64(n)
		expectOff += uint64(n)

		fmt.Printf("No%d: offset=%d 写入 %d bytes（本次累计 %d）\n", index, res.GetOffset(), n, total)
	}

	// 可选：更像生产（确保落盘；代价是性能会慢一些）
	if err := file.Sync(); err != nil {
		log.Fatalf("sync failed: %v", err)
	}

	fmt.Println("下载完成，保存到：", outPath)
}
