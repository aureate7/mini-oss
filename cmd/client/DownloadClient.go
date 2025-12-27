package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	ossv1 "github.com/aureate7/mini-oss/service_proto/pb/oss/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	addr := "127.0.0.1:8080"

	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("could not connect: %v", err)
	}
	defer conn.Close()

	client := ossv1.NewDownloadServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	stream, err := client.Download(ctx, &ossv1.DownloadRequest{
		ObjectId:    "1.PNG",
		StartOffset: 0,
		//EndOffset:   0,
		HasEnd:             false,
		PreferredChunkSize: 256 * 1024,
	})
	if err != nil {
		log.Fatalf("could not download: %v", err)
	}

	outPath := filepath.Join("static", "downloads", "1.PNG")
	file, err := os.OpenFile(outPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		log.Fatalf("could not open file: %v", err)
	}
	defer file.Close()

	writer := bufio.NewWriterSize(file, 256*1024)

	var index int
	var total int

	for {
		res, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("could not receive: %v", err)
		}

		index++

		n, err := writer.Write(res.Payload)
		if err != nil {
			log.Fatalf("could not write: %v", err)
		}
		if n != len(res.Payload) {
			log.Fatalf("could not write: partial write")
		}
		total += n
		fmt.Printf("No%d: 写入 %d bytes（累计 %d）\n", index, n, total)
	}

	if err := writer.Flush(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("下载完成，保存到：", outPath)
}
