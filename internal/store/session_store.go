package store

import "time"

type UploadSession struct {
	ObjectId       string
	UploadID       string
	FileName       string
	FileSize       uint64
	ChunkSize      uint32
	UploadedOffset uint64
	Sha256         string

	CreateTime time.Time
	UpdateTime time.Time
}
