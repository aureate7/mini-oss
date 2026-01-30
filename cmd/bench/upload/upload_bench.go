// cmd/bench_upload/main.go
/*
================ sha256方案：上传完成后全文件扫盘校验 =====================
2026/01/29 14:30:52 UPLOAD BENCH DONE
2026/01/29 14:30:52   concurrency=100 file=256MiB chunk=262144B keep=false
2026/01/29 14:30:52   success=100 fail=0 total_bytes=26843545600
2026/01/29 14:30:52   aggregate_throughput=442.70 MiB/s
2026/01/29 14:30:52   p99_latency=57310 ms (end-to-end RPC)

本地 loopback 环境下，256MiB 文件、chunk=256KiB、100 并发上传，聚合吞吐 ≈443 MiB/s
单次 Upload RPC（端到端）p99 延迟 ≈57s，100% 请求成功，无内存泄漏
上传完成后进行整文件 SHA256 校验并原子提交（rename + link），保证数据一致性

================ sha256方案：流式写入在线增量计算 + 断线续传前缀补算 =====================
2026/01/30 21:12:28 UPLOAD BENCH DONE
2026/01/30 21:12:28   concurrency=100 file=256MiB chunk=262144B keep=false
2026/01/30 21:12:28   success=100 fail=0 total_bytes=26843545600
2026/01/30 21:12:28   aggregate_throughput=1916.01 MiB/s
2026/01/30 21:12:28   p99_latency=12706 ms (end-to-end RPC)

本地环境 100 并发上传 256MiB 文件，chunk=256KiB，错误率 0%；上传聚合吞吐 1916 MiB/s，端到端 p99=12.7s。
通过将 SHA256 从“完成后全文件扫盘校验”优化为“流式写入在线增量计算 + 断线续传前缀补算”，并将 WriteAt 改为顺序 Write、降低 session 锁更新频率，使吞吐提升 4.3×、p99 降低 77.8%。

chunk	aggregate throughput	p99 latency
256KiB	1916.01 MiB/s	12.706 s
512KiB	2219.48 MiB/s	11.193 s
1MiB	2484.34 MiB/s	9.949 s
*/
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	ossv1 "github.com/aureate7/mini-oss/service_proto/pb/oss/v1"
)

func main() {
	var (
		addr     = flag.String("addr", "127.0.0.1:8080", "gRPC server address")
		con      = flag.Int("c", 100, "concurrency")
		chunk    = flag.Int("chunk", 1024*1024, "preferred chunk size")
		timeout  = flag.Duration("timeout", 15*time.Minute, "overall timeout")
		keep     = flag.Bool("keep", false, "keep uploaded objects on disk (default false)")
		filePath = flag.String("file", "/Users/aureate7/Downloads/gcc-master.zip", "source file path (will be created if not exists)")
		prefix   = flag.String("prefix", "bench_obj", "object_id prefix")
	)
	flag.Parse()

	// 1) 准备 256MiB 文件（不存在则创建）
	const size = 256 * 1024 * 1024 // 256MiB
	if err := ensureFile(*filePath, size); err != nil {
		log.Fatalf("ensureFile failed: %v", err)
	}
	shaHex, err := fileSHA256Hex(*filePath)
	if err != nil {
		log.Fatalf("sha256 failed: %v", err)
	}
	log.Printf("bench file=%s size=%d sha256=%s", *filePath, size, shaHex)

	// 2) gRPC 连接
	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	sessCli := ossv1.NewUploadSessionServiceClient(conn)
	upCli := ossv1.NewUploadServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	// 3) 并发压测
	startAll := time.Now()
	var totalBytes uint64
	lat := make([]time.Duration, *con)
	errs := make([]error, *con)

	var wg sync.WaitGroup
	wg.Add(*con)

	for i := 0; i < *con; i++ {
		i := i
		go func() {
			defer wg.Done()

			objectID := fmt.Sprintf("%s_%d_%d", *prefix, time.Now().UnixNano(), i)
			fileName := filepath.Base(*filePath)

			t0 := time.Now()
			// Create session
			sresp, err := sessCli.CreateUploadSession(ctx, &ossv1.CreateUploadSessionRequest{
				ObjectId:       objectID,
				FileName:       fileName,
				FileSize:       uint64(size),
				Sha256:         shaHex,
				PreferredChunk: uint32(*chunk),
			})
			if err != nil {
				errs[i] = fmt.Errorf("CreateUploadSession: %w", err)
				lat[i] = time.Since(t0)
				return
			}
			sess := sresp.GetSession()
			if sess == nil {
				errs[i] = fmt.Errorf("CreateUploadSession: nil session")
				lat[i] = time.Since(t0)
				return
			}

			// Upload stream
			stream, err := upCli.Upload(ctx)
			if err != nil {
				errs[i] = fmt.Errorf("Upload(): %w", err)
				lat[i] = time.Since(t0)
				return
			}

			f, err := os.Open(*filePath)
			if err != nil {
				_ = stream.CloseSend()
				errs[i] = fmt.Errorf("open file: %w", err)
				lat[i] = time.Since(t0)
				return
			}
			defer f.Close()

			// 断点续传：从 session.offset 开始
			offset := sess.GetOffset()
			if offset > uint64(size) {
				_ = stream.CloseSend()
				errs[i] = fmt.Errorf("invalid offset: %d > %d", offset, size)
				lat[i] = time.Since(t0)
				return
			}
			if _, err := f.Seek(int64(offset), io.SeekStart); err != nil {
				_ = stream.CloseSend()
				errs[i] = fmt.Errorf("seek: %w", err)
				lat[i] = time.Since(t0)
				return
			}

			buf := make([]byte, int(sess.GetChunkSize()))
			if len(buf) <= 0 {
				buf = make([]byte, *chunk)
			}

			for {
				n, rerr := f.Read(buf)
				if n > 0 {
					payload := buf[:n]
					isLast := (offset + uint64(n)) == uint64(size)

					ch := &ossv1.UploadChunk{
						UploadId:   sess.GetUploadId(),
						Offset:     offset,
						Payload:    payload,
						Checksum32: crc32.ChecksumIEEE(payload),
						IsLast:     isLast,
					}
					if err := stream.Send(ch); err != nil {
						_ = stream.CloseSend()
						errs[i] = fmt.Errorf("Send(offset=%d): %w", offset, err)
						lat[i] = time.Since(t0)
						return
					}
					offset += uint64(n)
				}
				if rerr == io.EOF {
					break
				}
				if rerr != nil {
					_ = stream.CloseSend()
					errs[i] = fmt.Errorf("read file: %w", rerr)
					lat[i] = time.Since(t0)
					return
				}
			}

			// recv result
			_, err = stream.CloseAndRecv()
			if err != nil {
				errs[i] = fmt.Errorf("CloseAndRecv: %w", err)
				lat[i] = time.Since(t0)
				return
			}

			atomic.AddUint64(&totalBytes, uint64(size))
			lat[i] = time.Since(t0)

			// 可选：清理对象文件，防止硬盘被堆满
			if !*keep {
				cleanupObject(objectID)
			}
		}()
	}

	wg.Wait()
	elapsedAll := time.Since(startAll)

	// 4) 统计输出
	okCount := 0
	failCount := 0
	for _, e := range errs {
		if e != nil {
			failCount++
		} else {
			okCount++
		}
	}

	// 只统计成功请求的延迟
	var okLat []time.Duration
	for i := 0; i < len(lat); i++ {
		if errs[i] == nil {
			okLat = append(okLat, lat[i])
		}
	}
	sort.Slice(okLat, func(i, j int) bool { return okLat[i] < okLat[j] })

	p99 := quantile(okLat, 0.99)
	throughputMiB := float64(totalBytes) / (1024.0 * 1024.0) / elapsedAll.Seconds()

	log.Printf("UPLOAD BENCH DONE")
	log.Printf("  concurrency=%d file=256MiB chunk=%dB keep=%v", *con, *chunk, *keep)
	log.Printf("  success=%d fail=%d total_bytes=%d", okCount, failCount, totalBytes)
	log.Printf("  aggregate_throughput=%.2f MiB/s", throughputMiB)
	if len(okLat) > 0 {
		log.Printf("  p99_latency=%d ms (end-to-end RPC)", p99.Milliseconds())
	}
	if failCount > 0 {
		log.Printf("  sample_error=%v", firstErr(errs))
	}
}

func ensureFile(path string, size int) error {
	fi, err := os.Stat(path)
	if err == nil && fi.Size() == int64(size) {
		return nil
	}
	if err == nil && fi.Size() != int64(size) {
		_ = os.Remove(path)
	}

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil && filepath.Dir(path) != "." {
		return err
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()

	// 稀疏文件：truncate 不会写满内容，速度快（本地压测足够）
	if err := f.Truncate(int64(size)); err != nil {
		return err
	}
	// 写一点随机性/非全0，避免某些压缩/去重影响（轻量）
	b := make([]byte, 1<<20) // 1MiB
	for i := range b {
		b[i] = byte((i*131 + 7) % 251)
	}
	_, _ = f.WriteAt(b, 0)
	_, _ = f.WriteAt(b, int64(size)-int64(len(b)))
	return nil
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

func cleanupObject(objectID string) {
	// 选择2结构：
	// index: static/index/<object_id> (symlink)
	// real : static/objects/<object_id>_<filename>_<upload_id>.<ext> (unknown name)
	indexPath := filepath.Join("static", "index", objectID)

	// 读链接目标（相对路径 ../objects/xxx）
	target, err := os.Readlink(indexPath)
	if err == nil {
		// target 是相对 index 的路径
		realPath := filepath.Join(filepath.Dir(indexPath), target)
		_ = os.Remove(realPath)
	}
	_ = os.Remove(indexPath)

	// 可选：清理 objects 下残留 part 文件（按前缀匹配）
	_ = filepath.WalkDir(filepath.Join("static", "objects"), func(p string, d os.DirEntry, err error) error {
		if err != nil || d == nil {
			return nil
		}
		// 删除 object_id 开头且包含 ".part." 的临时文件
		if d.Type().IsRegular() {
			name := d.Name()
			if strings.HasPrefix(name, objectID+"_") && strings.Contains(name, ".part.") {
				_ = os.Remove(p)
			}
		}
		return nil
	})
}

func quantile(sorted []time.Duration, q float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	if q <= 0 {
		return sorted[0]
	}
	if q >= 1 {
		return sorted[len(sorted)-1]
	}
	pos := int(math.Ceil(q*float64(len(sorted)))) - 1
	if pos < 0 {
		pos = 0
	}
	if pos >= len(sorted) {
		pos = len(sorted) - 1
	}
	return sorted[pos]
}

func firstErr(errs []error) error {
	for _, e := range errs {
		if e != nil {
			return e
		}
	}
	return nil
}
