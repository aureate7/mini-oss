package main

import (
	"io"
	"log"
	"net"
	"os"
	"path/filepath"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	ossv1 "github.com/aureate7/mini-oss/service_proto/pb/oss/v1"
)

type DownloadOss struct {
	ossv1.UnimplementedDownloadServiceServer
}
type UploadSessionOss struct {
	ossv1.UnimplementedUploadSessionServiceServer
}
type UploadOss struct {
	ossv1.UnimplementedUploadServiceServer
}

// Download 用户下载文件，服务端，流式传消息给用户端 若要看参数可去对应的grpc.pb.go文件看
func (d *DownloadOss) Download(req *ossv1.DownloadRequest, stream ossv1.DownloadService_DownloadServer) error {
	//log.Printf("Download Request: %v", req)

	// ========== helper: map fs error to grpc status ==========
	mapFSErr := func(err error, notFoundMsg string) error {
		switch {
		case err == nil:
			return nil
		case os.IsNotExist(err):
			return status.Error(codes.NotFound, notFoundMsg)
		case os.IsPermission(err):
			return status.Error(codes.PermissionDenied, "permission denied")
		default:
			return status.Error(codes.Internal, "filesystem error: "+err.Error())
		}
	}

	minU64 := func(a, b uint64) uint64 {
		if a < b {
			return a
		}
		return b
	}

	clamp := func(x, lo, hi int) int {
		if x < lo {
			return lo
		}
		if x > hi {
			return hi
		}
		return x
	}

	// 关卡1：参数
	objectId := req.GetObjectId()
	if objectId == "" {
		return status.Error(codes.InvalidArgument, "ObjectId is empty")
	}
	startOffset := req.GetStartOffset()
	hasEnd := req.GetHasEnd() // 表示是否使用客户端的endOffset
	endOffset := req.GetEndOffset()
	// 若客户端没传EndOffset则endOffset为0
	if hasEnd && startOffset >= endOffset {
		return status.Error(codes.InvalidArgument, "invalid range: start_offset >= end_offset")
	}

	// 关卡2 获取文件基本信息
	path := filepath.Join("static", "objects", objectId)
	fileInfo, err := os.Stat(path)
	/*
		os.Stat 返回的是 FileInfo 接口。它的操作非常轻量，因为它不需要读取文件内容，只是向操作系统请求文件的元数据（Metadata）。
		常用场景：
		检查文件是否存在。
		获取文件大小（用于计算下载的 end_offset）。
		获取修改时间（用于判断缓存是否过期）。
		判断是文件还是目录。
	*/
	if err != nil {
		return mapFSErr(err, "object not found")
	}
	size := uint64(fileInfo.Size())
	if startOffset > size {
		return status.Error(codes.InvalidArgument, "Start offset out of range")
	}

	// 关卡3 确认实际边界 裁剪
	end := size
	if hasEnd {
		end = minU64(endOffset, size) // 文件实际结尾处
	}
	// 一致性校验 防止协议语义变化时埋雷
	if end < startOffset {
		return status.Error(codes.InvalidArgument, "invalid range: end offset < start_offset")
	}

	// 关卡4 打开文件
	f, err := os.Open(path)
	if err != nil {
		return mapFSErr(err, "object not found")
	}
	defer f.Close()
	if _, err = f.Seek(int64(startOffset), io.SeekStart); err != nil {
		log.Printf("failed to seek file to %d: %v", startOffset, err)
		return status.Error(codes.Internal, "failed to prepare file stream: %v"+err.Error())
	}

	// 关卡5  chunk size

	const (
		defaultChunk = 256 * 1024
		minChunk     = 4 * 1024
		maxChunk     = 4 * 1024 * 1024
	)
	preferred := int(req.GetPreferredChunkSize())
	if preferred <= 0 {
		preferred = defaultChunk
	}
	chunkSize := clamp(preferred, minChunk, maxChunk)
	log.Printf("Download start object_id=%s start_offset=%d end=%d chunk_size=%d path=%s", objectId, startOffset, end, chunkSize, path)
	buf := make([]byte, chunkSize)
	offset := startOffset
	for offset < end {
		// 客户端取消预案
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		default:
		}
		remaining := end - offset
		toReadU64 := minU64(uint64(len(buf)), remaining) // 读操作的上限，应该以『实际 buffer 容量』为准，而不是『逻辑 chunkSize 变量』。
		toRead := int(toReadU64)

		n, rerr := f.Read(buf[:toRead])
		if n > 0 {
			if sendErr := stream.Send(&ossv1.FileChunk{
				Payload: buf[:n],
				Offset:  offset,
			}); sendErr != nil {
				return sendErr
			}
			offset += uint64(n)
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			return status.Error(codes.Internal, "Read failed: "+rerr.Error())
		}
		if n == 0 {
			return status.Error(codes.Internal, "read returned 0 bytes without EOF")
		}
	}

	return nil
}

func main() {
	lis, err := net.Listen("tcp", ":8080")
	if err != nil {
		panic(err)
	}
	grpcServer := grpc.NewServer()
	ossv1.RegisterDownloadServiceServer(grpcServer, &DownloadOss{})
	ossv1.RegisterUploadServiceServer(grpcServer, &UploadOss{})
	ossv1.RegisterUploadSessionServiceServer(grpcServer, &UploadSessionOss{})
	log.Println("mini-oss gRPC server started at :8080")
	if err := grpcServer.Serve(lis); err != nil {
		panic(err)
	}
}
