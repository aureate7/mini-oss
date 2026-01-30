package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/aureate7/mini-oss/internal/store"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	ossv1 "github.com/aureate7/mini-oss/service_proto/pb/oss/v1"
)

type DownloadOss struct {
	ossv1.UnimplementedDownloadServiceServer
}
type UploadSessionOss struct {
	ossv1.UnimplementedUploadSessionServiceServer

	mu         sync.RWMutex
	byUploadID map[string]*store.UploadSession
	byIdemKey  map[string]*store.UploadSession
}
type UploadOss struct {
	ossv1.UnimplementedUploadServiceServer

	sess *UploadSessionOss
}

func clamp(x, lo, hi int) int {
	if x < lo {
		return lo
	}
	if x > hi {
		return hi
	}
	return x
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

	//clamp := func(x, lo, hi int) int {
	//	if x < lo {
	//		return lo
	//	}
	//	if x > hi {
	//		return hi
	//	}
	//	return x
	//}

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
	//path := filepath.Join("static", "objects", objectId)

	// Download 统一从索引入口读取：static/index/<object_id>
	// 该路径是一个 symlink，指向 static/objects 下的真实文件名（含 filename + upload_id）
	path := filepath.Join("static", "index", objectId)

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

	// 并行分片+随机访问所需
	//if _, err = f.Seek(int64(startOffset), io.SeekStart); err != nil {
	//	log.Printf("failed to seek file to %d: %v", startOffset, err)
	//	return status.Error(codes.Internal, "failed to prepare file stream: %v"+err.Error())
	//}

	// 顺序下载/流式：SectionReader + Read 更简单、更快、更稳
	// 并行分片/随机访问：ReadAt 更灵活，但更复杂，顺序吞吐未必更好

	// 关卡5  chunk size

	const (
		defaultChunk = 256 * 1024
		minChunk     = 4 * 1024
		maxChunk     = 4 * 1024 * 1024
	)
	// 设定切片大小
	preferred := int(req.GetPreferredChunkSize())
	if preferred <= 0 {
		preferred = defaultChunk
	}
	chunkSize := clamp(preferred, minChunk, maxChunk)
	log.Printf("Download start object_id=%s start_offset=%d end=%d chunk_size=%d path=%s", objectId, startOffset, end, chunkSize, path)
	buf := make([]byte, chunkSize)

	sr := io.NewSectionReader(f, int64(startOffset), int64(end-startOffset)) // 参数1:文件指针；参数2:起始offset；参数3:要读取的总大小
	offset := startOffset
	for {
		// 客户端取消预案 cancel
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		default:
		}
		//remaining := end - offset
		//toReadU64 := minU64(uint64(len(buf)), remaining) // 读操作的上限，应该以『实际 buffer 容量』为准，而不是『逻辑 chunkSize 变量』。
		//toRead := int(toReadU64)
		//n, rerr := f.Read(buf[:toRead])
		n, rerr := sr.Read(buf)
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

func sanitizeFileName(name string) string {
	name = filepath.Base(name) // 去掉路径，只保留最后一段
	// 替换掉路径分隔符和空白等
	name = strings.ReplaceAll(name, "/", "_")
	name = strings.ReplaceAll(name, "\\", "_")
	name = strings.ReplaceAll(name, " ", "_")
	return name
}

func (u *UploadOss) Upload(stream ossv1.UploadService_UploadServer) error {
	ctx := stream.Context()

	// ===================== 1. Recv 第一帧，做基础校验 =====================
	first, err := stream.Recv()
	if err != nil {
		if err == io.EOF {
			return status.Error(codes.InvalidArgument, "empty upload stream")
		}
		return status.Error(codes.Unknown, "recv first chunk failed: "+err.Error())
	}

	uploadId := first.GetUploadId()
	if uploadId == "" {
		return status.Error(codes.InvalidArgument, "empty upload id")
	}
	if len(first.GetPayload()) == 0 {
		return status.Error(codes.InvalidArgument, "empty upload payload")
	}

	// ===================== 2. 获取 UploadSession =====================
	if u.sess == nil {
		return status.Error(codes.Internal, "session service not initialized")
	}
	sess, err := u.sess.getSessionByUploadID(uploadId)
	if err != nil {
		return err
	}

	// ===================== 3. 准备目录与文件路径 =====================
	objectsDir := filepath.Join("static", "objects")
	indexDir := filepath.Join("static", "index")
	if err := os.MkdirAll(objectsDir, 0o755); err != nil {
		return status.Error(codes.Internal, "mkdir objects dir failed")
	}
	if err := os.MkdirAll(indexDir, 0o755); err != nil {
		return status.Error(codes.Internal, "mkdir index dir failed")
	}

	safeName := sanitizeFileName(sess.FileName)
	ext := filepath.Ext(safeName)
	stem := strings.TrimSuffix(safeName, ext)

	finalName := fmt.Sprintf("%s_%s_%s%s", sess.ObjectId, stem, sess.UploadID, ext)
	finalPath := filepath.Join(objectsDir, finalName)

	tmpName := fmt.Sprintf("%s_%s.part.%s%s", sess.ObjectId, stem, sess.UploadID, ext)
	tmpPath := filepath.Join(objectsDir, tmpName)

	indexPath := filepath.Join(indexDir, sess.ObjectId)

	// ===================== 4. 打开/准备临时文件 =====================
	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return status.Error(codes.Internal, "open temp file failed: "+err.Error())
	}
	defer f.Close()

	// 【修改点①】仅在必要时 truncate，避免续传时重复 truncate
	if st, err := f.Stat(); err == nil {
		if st.Size() != int64(sess.FileSize) {
			if err := f.Truncate(int64(sess.FileSize)); err != nil {
				return status.Error(codes.Internal, "truncate file failed: "+err.Error())
			}
		}
	} else {
		if err := f.Truncate(int64(sess.FileSize)); err != nil {
			return status.Error(codes.Internal, "truncate file failed: "+err.Error())
		}
	}

	// ===================== 5. 初始化 expected offset 与 sha256 =====================
	expected := uint64(sess.UploadedOffset)
	hasher := sha256.New()

	// 【修改点②】断线重连续传：补算前缀 hash（只算已上传部分）
	if expected > 0 {
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			return status.Error(codes.Internal, "seek prefix failed: "+err.Error())
		}
		if _, err := io.CopyN(hasher, f, int64(expected)); err != nil {
			return status.Error(codes.Internal, "sha256 prefix calc failed: "+err.Error())
		}
	}

	// 【修改点③】顺序写：Seek 到 expected，后续使用 Write（而不是 WriteAt）
	if _, err := f.Seek(int64(expected), io.SeekStart); err != nil {
		return status.Error(codes.Internal, "seek write start failed: "+err.Error())
	}

	// ===================== 6. 定义 chunk 处理逻辑 =====================
	var lastUpdate uint64

	handleChunk := func(ch *ossv1.UploadChunk) error {
		if ctx.Err() != nil {
			return status.Error(codes.Canceled, "request canceled")
		}
		if ch.GetUploadId() != uploadId {
			return status.Error(codes.InvalidArgument, "mixed upload_id in stream")
		}

		payload := ch.GetPayload()
		if len(payload) == 0 {
			return status.Error(codes.InvalidArgument, "empty upload payload")
		}
		if sess.ChunkSize > 0 && uint32(len(payload)) > sess.ChunkSize {
			return status.Errorf(codes.InvalidArgument, "chunk too large")
		}

		// CRC32 校验（可选）
		if ch.GetChecksum32() != 0 {
			if crc32.ChecksumIEEE(payload) != ch.GetChecksum32() {
				return status.Error(codes.DataLoss, "chunk checksum mismatch")
			}
		}

		off := ch.GetOffset()

		// 幂等：重复 chunk 直接忽略
		if off < expected {
			return nil
		}
		// 顺序约束
		if off > expected {
			return status.Errorf(codes.FailedPrecondition, "unexpected offset: %d != %d", off, expected)
		}

		// 【修改点④】顺序写 + 在线 hash
		n, err := f.Write(payload)
		if err != nil {
			return status.Error(codes.Internal, "write failed: "+err.Error())
		}
		if n != len(payload) {
			return status.Error(codes.Internal, "short write")
		}
		_, _ = hasher.Write(payload)

		expected += uint64(n)

		// 【修改点⑤】session offset 更新降频（减少锁竞争）
		if expected-lastUpdate >= 4*1024*1024 || ch.GetIsLast() {
			u.sess.mu.Lock()
			if expected > sess.UploadedOffset {
				sess.UploadedOffset = expected
				sess.UpdateTime = time.Now()
			}
			u.sess.mu.Unlock()
			lastUpdate = expected
		}

		if ch.GetIsLast() && expected != sess.FileSize {
			return status.Errorf(codes.FailedPrecondition,
				"expected file size %d, got %d", sess.FileSize, expected)
		}
		return nil
	}

	// ===================== 7. 处理第一帧 + 后续帧 =====================
	if err := handleChunk(first); err != nil {
		return err
	}

	for {
		ch, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			return status.Error(codes.Unknown, "recv chunk failed: "+err.Error())
		}
		if err := handleChunk(ch); err != nil {
			return err
		}
		if ch.GetIsLast() && expected == sess.FileSize {
			break
		}
	}

	// ===================== 8. 完整性校验 =====================
	if expected != sess.FileSize {
		return status.Errorf(codes.FailedPrecondition,
			"upload incomplete: %d/%d", expected, sess.FileSize)
	}

	// 【修改点⑥】不再扫全盘，直接使用在线 sha256
	finalSha := hex.EncodeToString(hasher.Sum(nil))
	if sess.Sha256 != "" && finalSha != sess.Sha256 {
		return status.Error(codes.DataLoss, "file sha256 mismatch")
	}

	// ===================== 9. 原子提交 + 建立索引 =====================
	if err := os.Rename(tmpPath, finalPath); err != nil {
		return status.Error(codes.Internal, "rename failed: "+err.Error())
	}
	_ = os.Remove(indexPath)
	target := filepath.Join("..", "objects", finalName)
	if err := os.Symlink(target, indexPath); err != nil {
		return status.Error(codes.Internal, "symlink failed: "+err.Error())
	}

	// ===================== 10. 返回结果 =====================
	return stream.SendAndClose(&ossv1.UploadFileResult{
		ObjectId:    sess.ObjectId,
		UploadId:    sess.UploadID,
		Size:        sess.FileSize,
		Sha256:      finalSha,
		CompletedAt: timestamppb.New(time.Now()),
	})
}

func (u *UploadOss) Upload_back(stream ossv1.UploadService_UploadServer) error {
	ctx := stream.Context()

	// ===================== 1. Recv 第一帧，做基础校验 =====================
	// 1) 先Recv第一帧，用它做强校验+绑定session
	first, err := stream.Recv()
	if err != nil {
		if err == io.EOF {
			return status.Error(codes.InvalidArgument, "empty upload stream")
		}
		return status.Error(codes.Unknown, "recv first chunk failed: "+err.Error())
	}

	uploadId := first.GetUploadId()
	if uploadId == "" {
		return status.Error(codes.InvalidArgument, "empty upload id")
	}
	//if first.GetOffset() < 0 {
	//	return status.Error(codes.InvalidArgument, "offset is negative")
	//}
	if len(first.GetPayload()) == 0 {
		return status.Error(codes.InvalidArgument, "empty upload payload")
	}

	// ===================== 2. 获取 UploadSession =====================
	// 2) 通过upload_id找session (upload不负责创建session)
	if u.sess == nil {
		return status.Error(codes.Internal, "session service not initialized")
	}
	sess, err := u.sess.getSessionByUploadID(uploadId)
	if err != nil {
		//return status.Error(codes.NotFound, "upload session not found")
		return err // 保留原始错误码
	}
	if sess == nil {
		return status.Error(codes.NotFound, "upload session not found")
	}

	// 若proto里也带object_id/file_size等，校验一致性
	//if uint32(first.GetChecksum32()) != 0 {
	//	crc := crc32.ChecksumIEEE(first.GetPayload())
	//	if crc != first.GetChecksum32() {
	//		return status.Error(codes.DataLoss, "Data loss detected")
	//	}
	//}

	// 3) 打开落盘文件 用object_id+upload_id+time.Now()作为文件名，避免冲突
	// 最终文件路径: static/objects/<object_id>
	// 临时文件路径: static/objects/<object_id>.part.<upload_id>
	//baseDir := filepath.Join("static", "objects")
	//path := filepath.Join(baseDir, sess.FileName, sess.ObjectId, sess.UploadID, time.Now().Format(time.RFC3339))
	//if err := os.MkdirAll(baseDir, 0o755); err != nil {
	//	return status.Error(codes.Internal, "mkdir failed: "+err.Error())
	//}
	// ===================== 3. 准备目录与文件路径 =====================
	objectsDir := filepath.Join("static", "objects")
	indexDir := filepath.Join("static", "index")
	if err := os.MkdirAll(objectsDir, 0o755); err != nil {
		return status.Error(codes.Internal, "mkdir objects dir failed: "+objectsDir)
	}
	if err := os.MkdirAll(indexDir, 0o755); err != nil {
		return status.Error(codes.Internal, "mkdir index dir failed: "+objectsDir)
	}

	safeName := sanitizeFileName(sess.FileName)
	ext := filepath.Ext(safeName)             // .zip
	stem := strings.TrimSuffix(safeName, ext) // gcc-master

	// 真实文件名：<object_id>_<filename>_<upload_id>.<ext>
	finalName := fmt.Sprintf("%s_%s_%s%s", sess.ObjectId, stem, sess.UploadID, ext)
	finalPath := filepath.Join(objectsDir, finalName)

	// 临时文件名：<object_id>_<filename>.part.<upload_id>.<ext>(同目录，确保rename原子)
	tmpName := fmt.Sprintf("%s_%s.part.%s%s", sess.ObjectId, stem, sess.UploadID, ext)
	tmpPath := filepath.Join(objectsDir, tmpName)

	// 索引入口：static/index/<object_id> (syslink -> ../objects/<finalName>)
	indexPath := filepath.Join(indexDir, sess.ObjectId)

	// ===================== 4. 打开/准备临时文件 =====================
	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_RDWR, 0o644) // 0o八进制
	if err != nil {
		return status.Error(codes.Internal, "open file failed: "+err.Error())
	}
	defer f.Close()

	// 预分配文件大小：提前暴露磁盘空间问题，减少碎片
	// 只有新文件 or 大小不对时才会truncate，更稳定
	if st, err := f.Stat(); err == nil {
		if st.Size() != int64(sess.FileSize) {
			if err := f.Truncate(int64(sess.FileSize)); err != nil {
				return status.Error(codes.Internal, "truncate file failed_1: "+err.Error())
			}
		}
	} else {
		if err := f.Truncate(int64(sess.FileSize)); err != nil {
			return status.Error(codes.Internal, "truncate file failed_2: "+err.Error())
		}
	}

	// v1
	//if sess.FileSize > 0 {
	//	if err := f.Truncate(int64(sess.FileSize)); err != nil {
	//		return status.Error(codes.Internal, "truncate file failed: "+err.Error())
	//	}
	//}

	// ===================== 5. 初始化 expected offset 与 sha256 =====================
	// 4) 以session的uploadedOffset作为最终进度
	expected := uint64(sess.UploadedOffset)
	hasher := sha256.New()
	// 断点续传（优化版）：补算前缀hash（只算已上传部分）
	if expected > 0 {
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			return status.Error(codes.Internal, "seek prefix failed: "+err.Error())
		}
		if _, err := io.CopyN(hasher, f, int64(expected)); err != nil {
			return status.Error(codes.Internal, "sha256 prefix calc failed: "+err.Error())
		}
	}
	// 顺序写：Seek到expected，后续使用Write（而不是WriteAt）
	if _, err := f.Seek(int64(expected), io.SeekStart); err != nil {
		return status.Error(codes.Internal, "seek write start failed: "+err.Error())
	}

	// ===================== 6. 定义 chunk 处理逻辑 =====================
	var lastUpdate uint64
	// 函数定义：处理每个chunk：校验offset -> crc32(可选) -> 写入 -> 更新expected/session
	handleChunk := func(ch *ossv1.UploadChunk) error {
		// cancel/time out
		if ctx.Err() != nil {
			return status.Error(codes.Canceled, "request canceled")
		}
		// upload_id 一致性：防止同一条stream混入其他upload
		if ch.GetUploadId() != uploadId {
			return status.Error(codes.InvalidArgument, "mixed upload_id in one stream")
		}
		payload := ch.GetPayload()
		if len(payload) == 0 {
			return status.Error(codes.InvalidArgument, "empty upload payload")
		}
		// chunk大小上限(from session)
		if sess.ChunkSize > 0 && uint32(len(payload)) > sess.ChunkSize {
			return status.Errorf(codes.InvalidArgument, "chunk size too large: %d > %d", len(payload), sess.ChunkSize)
		}
		// CRC32 校验
		if ch.GetChecksum32() != 0 {
			crc := crc32.ChecksumIEEE(payload)
			if crc != ch.GetChecksum32() {
				return status.Error(codes.DataLoss, "chunk checksum mismatch")
			}
		}

		off := ch.GetOffset()

		// 幂等重试：重复chunk直接忽略
		if off < expected {
			return nil
		}
		// 顺序约束：必须连接，不允许跳跃（乱序/丢段）
		if off > expected {
			return status.Errorf(codes.FailedPrecondition, "unexpected offset: got=%d expected=%d", off, expected)
		}

		// 顺序写 + 在线hash
		n, err := f.Write(payload)
		if err != nil {
			return status.Error(codes.Internal, "write chunk failed: "+err.Error())
		}
		if n != len(payload) {
			return status.Error(codes.Internal, "short write chunk")
		}
		_, _ = hasher.Write(payload)

		expected += uint64(n)

		// session offset 更新降频（减少锁竞争） 上传更新超过4MB(4*1024*1024)才更新session
		if expected-lastUpdate >= 4*1024*1024 || ch.GetIsLast() {
			u.sess.mu.Lock()
			if expected > sess.UploadedOffset {
				sess.UploadedOffset = expected
				sess.UpdateTime = time.Now()
			}
			u.sess.mu.Unlock()
			lastUpdate = expected
		}

		// off == expected: 写入
		//n, werr := f.WriteAt(payload, int64(off))
		//if werr != nil {
		//	return status.Error(codes.Internal, "write chunk failed: "+werr.Error())
		//}
		//if n != len(payload) {
		//	return status.Errorf(codes.Internal, "short write chunk: got=%d expected=%d", n, len(payload))
		//}
		//
		//expected += uint64(n)
		//
		//// 更新session 高并发加锁
		//u.sess.mu.Lock()
		//if expected > sess.UploadedOffset {
		//	sess.UploadedOffset = expected
		//	sess.UpdateTime = time.Now()
		//}
		//u.sess.mu.Unlock()

		// is_last 校验：声明最后一块时，必须刚好到file_size
		if ch.GetIsLast() {
			if expected != sess.FileSize {
				return status.Errorf(
					codes.FailedPrecondition,
					"expected file size %d, got %d", expected, sess.FileSize,
				)
			}
		}
		return nil
	} // end of handleChunk

	// ===================== 7. 处理第一帧 + 后续帧 =====================
	// 先处理第一帧
	if err := handleChunk(first); err != nil {
		return err
	}

	// 5) 循环接收后续chunk
	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		default:
		}
		ch, rerr := stream.Recv()
		if rerr != nil {
			if rerr == io.EOF {
				break
			}
			return status.Error(codes.Unknown, "recv chunk failed: "+rerr.Error())
		}

		if err := handleChunk(ch); err != nil {
			return err
		}
		// 客户端标记最后一块且已完成，允许提前结束(避免客户端忘记closesend)
		if ch.GetIsLast() && expected == sess.FileSize {
			break
		}
	}

	// ===================== 8. 完整性校验 =====================
	// 6) 完成一致性：必须完整写入
	if sess.FileSize > 0 && expected != sess.FileSize {
		return status.Errorf(codes.FailedPrecondition, "upload incomplete: uploaded=%d total=%d", expected, sess.FileSize)
	}

	// sha256优化：在线sha256
	finalSha := hex.EncodeToString(hasher.Sum(nil))
	if sess.Sha256 != "" && finalSha != sess.Sha256 {
		return status.Error(codes.DataLoss, "sha256 checksum mismatch")
	}

	// 7) 整体SHA256校验 与session期望值对比 「主要的尾部延迟开销，需要优化」
	//if _, err := f.Seek(0, io.SeekStart); err != nil {
	//	return status.Error(codes.Internal, "seek failed: "+err.Error())
	//}
	//h := sha256.New()
	//if _, err := io.Copy(h, f); err != nil {
	//	return status.Error(codes.Internal, "sha256 calc failed: "+err.Error())
	//}
	//finalSha := hex.EncodeToString(h.Sum(nil))
	//if sess.Sha256 != "" && finalSha != sess.Sha256 {
	//	return status.Error(codes.DataLoss, "file sha256 mismatch")
	//}

	// ===================== 9. 原子提交 + 建立索引 =====================
	// 8) 原子提交：rename临时文件为最终文件(同目录，rename原子)
	if err := os.Rename(tmpPath, finalPath); err != nil {
		return status.Error(codes.Internal, "rename failed: "+err.Error())
	}

	// 通过索引入口兼容Download：static/index/<object_id> -> ../objects/<fileName>
	// 先清理旧索引（老文件/老链接）
	_ = os.Remove(indexPath)

	// 使用相对路径 syslink：index/<object_id> -> ../objects/<finalName>
	target := filepath.Join("..", "objects", finalName)
	if err := os.Symlink(target, indexPath); err != nil {
		return status.Error(codes.Internal, "symlink failed: "+err.Error())
	}

	// ===================== 10. 返回结果 =====================
	// 9) 返回结果
	resp := &ossv1.UploadFileResult{
		ObjectId:    sess.ObjectId,
		UploadId:    sess.UploadID,
		Size:        sess.FileSize,
		Sha256:      finalSha,
		CompletedAt: timestamppb.New(time.Now()),
	}

	return stream.SendAndClose(resp)
}

func toProtoUploadSession(s *store.UploadSession) *ossv1.UploadSession {
	return &ossv1.UploadSession{
		ObjectId:  s.ObjectId,
		UploadId:  s.UploadID,
		FileName:  s.FileName,
		FileSize:  s.FileSize,
		ChunkSize: s.ChunkSize,
		Offset:    s.UploadedOffset,
		Sha256:    s.Sha256,
		Status:    ossv1.UploadSessionStatus_UPLOAD_SESSION_STATUS_UPLOADING,
		CreatedAt: timestamppb.New(s.CreateTime),
		UpdatedAt: timestamppb.New(s.UpdateTime),
	}
}
func buildCreateUploadSessionResp(s *store.UploadSession, reused bool) *ossv1.CreateUploadSessionResponse {
	return &ossv1.CreateUploadSessionResponse{
		Session: toProtoUploadSession(s),
		Reused:  reused,
	}
}

// UploadSessionOss
//  1. 参数校验
//  2. 计算 chunkSize（clamp）
//  3. 判断幂等 key 是否已存在
//     ├─ 是 → reused=true，直接返回
//     └─ 否 → 创建新 session
//  4. 存入 store
//  5. 返回 proto.UploadSession
func (us *UploadSessionOss) getSessionByUploadID(uploadId string) (*store.UploadSession, error) {
	if uploadId == "" {
		return nil, status.Error(codes.InvalidArgument, "empty upload id")
	}

	us.mu.RLock()
	defer us.mu.RUnlock()
	if us.byUploadID == nil {
		return nil, status.Error(codes.Internal, "session store not initialized")
	}

	sess, ok := us.byUploadID[uploadId]
	if !ok || sess == nil {
		return nil, status.Error(codes.NotFound, "upload session not found")
	}
	return sess, nil
}

func (us *UploadSessionOss) CreateUploadSession(ctx context.Context, req *ossv1.CreateUploadSessionRequest) (*ossv1.CreateUploadSessionResponse, error) {
	// 校验
	objectId := req.GetObjectId()
	if objectId == "" {
		return nil, status.Error(codes.InvalidArgument, "ObjectId is empty")
	}
	fileName := req.GetFileName()
	if fileName == "" {
		return nil, status.Error(codes.InvalidArgument, "Filename is empty")
	}
	fileSize := req.GetFileSize()
	if fileSize == 0 {
		return nil, status.Error(codes.InvalidArgument, "FileSize is zero")
	}
	sha256 := req.GetSha256()
	if sha256 == "" {
		return nil, status.Error(codes.InvalidArgument, "sha256 is empty")
	}
	// ===== chunk size clamp =====
	const (
		defaultChunk = 256 * 1024
		minChunk     = 4 * 1024
		maxChunk     = 4 * 1024 * 1024
	)
	// 设定切片大小
	preferred := int(req.GetPreferredChunk())
	if preferred <= 0 {
		preferred = defaultChunk
	}
	chunkSize := clamp(preferred, minChunk, maxChunk)
	// ===== 幂等键：同一个文件 + size + sha256 = 同一个会话 =====
	idemKey := req.IdempotencyKey
	if idemKey == "" {
		idemKey = fmt.Sprintf("%s|%d|%s", objectId, fileSize, sha256)
	}
	// ===== 如果会话已存在，直接返回（幂等）=====
	us.mu.Lock()
	if sess, ok := us.byIdemKey[idemKey]; ok {
		us.mu.Unlock()
		return buildCreateUploadSessionResp(sess, true), nil
	}

	// 创建新 session
	uploadID := uuid.NewString()
	now := time.Now()

	sess := &store.UploadSession{
		ObjectId:       objectId,
		UploadID:       uploadID,
		FileName:       fileName,
		FileSize:       fileSize,
		ChunkSize:      uint32(chunkSize),
		UploadedOffset: 0,
		Sha256:         sha256,
		CreateTime:     now,
		UpdateTime:     now,
	}

	// maps are protected by us.mu
	us.byUploadID[uploadID] = sess
	us.byIdemKey[idemKey] = sess
	us.mu.Unlock()
	return buildCreateUploadSessionResp(sess, false), nil
}

func main() {
	lis, err := net.Listen("tcp", ":8080")
	if err != nil {
		panic(err)
	}
	grpcServer := grpc.NewServer()
	ossv1.RegisterDownloadServiceServer(grpcServer, &DownloadOss{})
	sessSvc := &UploadSessionOss{
		byUploadID: make(map[string]*store.UploadSession),
		byIdemKey:  make(map[string]*store.UploadSession),
	}
	ossv1.RegisterUploadSessionServiceServer(grpcServer, sessSvc)
	ossv1.RegisterUploadServiceServer(grpcServer, &UploadOss{
		sess: sessSvc,
	})
	//ossv1.RegisterUploadServiceServer(grpcServer, &UploadOss{})
	//ossv1.RegisterUploadSessionServiceServer(grpcServer, &UploadSessionOss{
	//	byUploadID: make(map[string]*store.UploadSession),
	//	byIdemKey:  make(map[string]*store.UploadSession),
	//})
	log.Println("mini-oss gRPC server started at :8080")
	if err := grpcServer.Serve(lis); err != nil {
		panic(err)
	}
}
