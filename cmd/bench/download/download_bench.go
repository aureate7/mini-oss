// cmd/bench_download/main.go
/*
2026/01/29 14:54:32 DOWNLOAD BENCH DONE
2026/01/29 14:54:32   concurrency=100 preferred_chunk=262144B object_id=obj_test_002
2026/01/29 14:54:32   success=100 fail=0 total_bytes=26843545600
2026/01/29 14:54:32   aggregate_throughput=3373.72 MiB/s
2026/01/29 14:54:32   p99_latency=7587 ms (end-to-end RPC)

本地 loopback 环境，256MiB 文件、chunk=256KiB、100 并发下载：聚合吞吐 ≈3373.7 MiB/s，端到端 p99 ≈7.6s，100% 成功
*/
package main

import (
	"context"
	"flag"
	"io"
	"log"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	ossv1 "github.com/aureate7/mini-oss/service_proto/pb/oss/v1"
)

func main() {
	var (
		addr    = flag.String("addr", "127.0.0.1:8080", "gRPC server address")
		con     = flag.Int("c", 100, "concurrency")
		chunk   = flag.Int("chunk", 256*1024, "preferred chunk size")
		timeout = flag.Duration("timeout", 10*time.Minute, "overall timeout")
		object  = flag.String("object", "obj_test_002", "object_id to download (must exist)")
	)
	flag.Parse()
	if *object == "" {
		log.Fatal("please provide -object=<object_id> (upload one 256MiB object first)")
	}

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	cli := ossv1.NewDownloadServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

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
			t0 := time.Now()

			stream, err := cli.Download(ctx, &ossv1.DownloadRequest{
				ObjectId:           *object,
				StartOffset:        0,
				HasEnd:             false,
				EndOffset:          0,
				PreferredChunkSize: uint32(*chunk),
			})
			if err != nil {
				errs[i] = err
				lat[i] = time.Since(t0)
				return
			}

			var bytes uint64
			for {
				ch, rerr := stream.Recv()
				if rerr == io.EOF {
					break
				}
				if rerr != nil {
					errs[i] = rerr
					lat[i] = time.Since(t0)
					return
				}
				p := ch.GetPayload()
				bytes += uint64(len(p))
				// 丢弃数据（不落盘）
				_, _ = io.Discard.Write(p)
			}

			atomic.AddUint64(&totalBytes, bytes)
			lat[i] = time.Since(t0)
		}()
	}

	wg.Wait()
	elapsedAll := time.Since(startAll)

	okCount := 0
	failCount := 0
	for _, e := range errs {
		if e != nil {
			failCount++
		} else {
			okCount++
		}
	}

	var okLat []time.Duration
	for i := 0; i < len(lat); i++ {
		if errs[i] == nil {
			okLat = append(okLat, lat[i])
		}
	}
	sort.Slice(okLat, func(i, j int) bool { return okLat[i] < okLat[j] })

	p99 := quantile(okLat, 0.99)
	throughputMiB := float64(totalBytes) / (1024.0 * 1024.0) / elapsedAll.Seconds()

	log.Printf("DOWNLOAD BENCH DONE")
	log.Printf("  concurrency=%d preferred_chunk=%dB object_id=%s", *con, *chunk, *object)
	log.Printf("  success=%d fail=%d total_bytes=%d", okCount, failCount, totalBytes)
	log.Printf("  aggregate_throughput=%.2f MiB/s", throughputMiB)
	if len(okLat) > 0 {
		log.Printf("  p99_latency=%d ms (end-to-end RPC)", p99.Milliseconds())
	}
	if failCount > 0 {
		log.Printf("  sample_error=%v", firstErr(errs))
	}
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
