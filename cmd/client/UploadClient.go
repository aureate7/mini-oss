package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"hash/crc32"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	ossv1 "github.com/aureate7/mini-oss/service_proto/pb/oss/v1"
)

func main() {
	file := "pytorch_model-00001-of-00002.bin"
	object := "obj_test_005"
	var (
		addr      = flag.String("addr", "127.0.0.1:8080", "gRPC server address")
		filePath  = flag.String("file", "/Users/aureate7/hf_models/Llama-2-7b-chat-hf/"+file, "local file path to upload")
		objectID  = flag.String("object", object, "object_id (final storage key)")
		fileName  = flag.String("name", file, "file name for session")
		preferred = flag.Int("chunk", 256*1024, "preferred chunk size")
		timeout   = flag.Duration("timeout", 5*time.Minute, "overall timeout")
	)
	flag.Parse()

	if *filePath == "" || *objectID == "" {
		log.Fatal("usage: go run . -file=<path> -object=<object_id> [-name=xxx] [-addr=ip:port]")
	}
	if *fileName == "" {
		*fileName = filepath.Base(*filePath)
	}

	// 读取文件信息
	fi, err := os.Stat(*filePath)
	if err != nil {
		log.Fatalf("stat file failed: %v", err)
	}
	fileSize := uint64(fi.Size())
	if fileSize == 0 {
		log.Fatalf("file is empty")
	}

	// 计算 sha256（用于 CreateUploadSession）
	shaHex, err := fileSHA256Hex(*filePath)
	if err != nil {
		log.Fatalf("calc sha256 failed: %v", err)
	}
	log.Printf("local file: %s size=%d sha256=%s", *filePath, fileSize, shaHex)

	// 连接 gRPC
	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	sessCli := ossv1.NewUploadSessionServiceClient(conn)
	upCli := ossv1.NewUploadServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	// 1) CreateUploadSession（幂等）
	createReq := &ossv1.CreateUploadSessionRequest{
		ObjectId:       *objectID,
		FileName:       *fileName,
		FileSize:       fileSize,
		Sha256:         shaHex,
		PreferredChunk: uint32(*preferred),
		IdempotencyKey: "", // 为空则服务端用 object_id|size|sha256
	}
	createResp, err := sessCli.CreateUploadSession(ctx, createReq)
	if err != nil {
		log.Fatalf("CreateUploadSession failed: %v", err)
	}
	sess := createResp.GetSession()
	log.Printf("session: upload_id=%s object_id=%s file=%s file_size=%d chunk_size=%d offset=%d reused=%v",
		sess.GetUploadId(), sess.GetObjectId(), sess.GetFileName(), sess.GetFileSize(), sess.GetChunkSize(),
		sess.GetOffset(), createResp.GetReused(),
	)

	// 2) Upload(stream)
	// 从 session.offset 开始续传
	startOff := sess.GetOffset()
	if startOff > fileSize {
		log.Fatalf("invalid session offset: %d > fileSize %d", startOff, fileSize)
	}
	if startOff == fileSize {
		log.Printf("already uploaded (offset == file_size). nothing to do.")
		return
	}

	f, err := os.Open(*filePath)
	if err != nil {
		log.Fatalf("open file failed: %v", err)
	}
	defer f.Close()

	// Seek 到 offset（断点续传）
	if _, err := f.Seek(int64(startOff), io.SeekStart); err != nil {
		log.Fatalf("seek failed: %v", err)
	}

	stream, err := upCli.Upload(ctx)
	if err != nil {
		log.Fatalf("Upload() failed to open stream: %v", err)
	}

	chunkSize := int(sess.GetChunkSize())
	if chunkSize <= 0 {
		chunkSize = *preferred
	}
	buf := make([]byte, chunkSize)

	offset := startOff
	for {
		n, rerr := f.Read(buf)
		if n > 0 {
			payload := buf[:n]
			// 判断是不是最后一块：offset + n == fileSize
			isLast := (offset + uint64(n)) == fileSize
			ch := &ossv1.UploadChunk{
				UploadId:   sess.GetUploadId(),
				Offset:     offset,
				Payload:    payload,
				Checksum32: crc32.ChecksumIEEE(payload),
				IsLast:     isLast,
			}
			if err := stream.Send(ch); err != nil {
				_ = stream.CloseSend()
				log.Fatalf("stream.Send failed (offset=%d n=%d): %v", offset, n, err)
			}
			offset += uint64(n)

			// 打印进度（可选）
			if offset%(16*1024*1024) < uint64(n) { // 每 16MB 打一次
				log.Printf("uploaded: %d/%d (%.2f%%)", offset, fileSize, float64(offset)*100.0/float64(fileSize))
			}
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			_ = stream.CloseSend()
			log.Fatalf("read file failed: %v", rerr)
		}
	}

	// 结束并接收服务端结果
	res, err := stream.CloseAndRecv()
	if err != nil {
		log.Fatalf("CloseAndRecv failed: %v", err)
	}
	log.Printf("UPLOAD OK: object_id=%s upload_id=%s size=%d sha256=%s completed_at=%v",
		res.GetObjectId(), res.GetUploadId(), res.GetSize(), res.GetSha256(), res.GetCompletedAt(),
	)
}

func fileSHA256Hex(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
