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

func main() {
	addr := "127.0.0.1:8080"

	//conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	//conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{})))  // 生产环境下使用TLS加密
	if err != nil {
		log.Fatalf("failed to create grpc client: %v", err)
	}
	defer conn.Close()

	client := ossv1.NewDownloadServiceClient(conn)
	//ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second) // 30s内未完成传输则会被取消/返回超时错误
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute) // 方便大文件续传
	defer cancel()

	const (
		objectID  = "1.PNG"
		start     = uint64(0)
		hasEnd    = false
		end       = uint64(0)  // hasEnd=false 时忽略
		chunkSize = 256 * 1024 // 也可以做 clamp
		outName   = "1.PNG"
		outDirRel = "static/downloads"
	)

	stream, err := client.Download(ctx, &ossv1.DownloadRequest{
		ObjectId:           objectID,
		StartOffset:        start,
		EndOffset:          end,
		HasEnd:             hasEnd,
		PreferredChunkSize: chunkSize,
	})
	if err != nil {
		log.Fatalf("could not download: %v", err)
	}

	outDir := filepath.Clean(outDirRel)
	if err := os.MkdirAll(outDir, 0755); err != nil {
		log.Fatalf("could not create out dir: %v", err)
	}
	outPath := filepath.Join(outDir, outName)
	file, err := os.OpenFile(outPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		log.Fatalf("could not open file: %v", err)
	}
	defer file.Close()

	// 用 offset 写入：就算未来服务端乱序/重试，你也能写对
	// bufio.Writer 对随机写不友好，所以这里直接用 file.WriteAt（更契合 offset 语义）
	//writer := bufio.NewWriterSize(file, 256*1024)

	var (
		index     int
		total     uint64
		expectOff = start // 用于连续性校验（你当前服务端是顺序的）
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
			log.Fatalf("could not reveive: %v", rerr)
		}
		index++

		// 连续性校验：当前服务端实现是顺序发送，这里校验能帮你抓 bug
		if res.GetOffset() != expectOff {
			log.Fatalf("expected offset %d, got %d", expectOff, res.GetOffset())
		}

		payload := res.GetPayload()
		if len(payload) == 0 {
			log.Fatalf("empty payload at offset %d", res.GetOffset())
		}

		// 按 offset 写入（WriteAt 不依赖 bufio，随机写也没问题）
		n, werr := file.WriteAt(payload, int64(res.GetOffset()))
		if werr != nil {
			log.Fatalf("could not write: %v", werr)
		}
		if n != len(payload) {
			log.Fatalf("partial write: wrote=%d want=%d", n, len(payload))
		}
		total += uint64(n)
		expectOff += uint64(n)

		fmt.Printf("No%d: offset=%d 写入 %d bytes（累计 %d）\n", index, res.GetOffset(), n, total)
	}

	// fsync（可选）：更像生产 作用：把 OS 缓存的数据尽量刷到磁盘（fsync）。
	if err := file.Sync(); err != nil {
		log.Fatalf("sync failed: %v", err)
	}
	fmt.Println("下载完成，保存到：", outPath)
}
