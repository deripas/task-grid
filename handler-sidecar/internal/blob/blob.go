// Package blob - adapter to blob storage for answers above the inline limit: an S3 bucket (SeaweedFS, MinIO, AWS)
// through minio-go; implements the port sidecar.Blobs.
//
// Format of an object: the answer compressed with zstd; size and sha256 in the reference are of the answer
// before compression, so that a reader checks what it has decoded. Objects are not deleted here: the bucket
// expires them by the TTL of the key prefix (lifecycle rule).
package blob

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"

	"github.com/klauspost/compress/zstd"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/deripas/task-grid/handler-sidecar/internal/domain"
)

// Encoding - of the stored objects.
const Encoding = "zstd"

// Config - an S3 bucket.
type Config struct {
	Endpoint  string // host:port; empty - no blob storage
	Bucket    string
	AccessKey string // empty with SecretKey - anonymous access
	SecretKey string
	Secure    bool   // https
	Region    string // empty - asked of the storage
	Prefix    string // of the keys, e.g. "results/1h/": the bucket expires objects by it
}

// Store puts answers into one bucket.
type Store struct {
	client  *minio.Client
	bucket  string
	prefix  string
	encoder *zstd.Encoder
}

// New creates a client of the bucket; no requests are made until the first Put.
func New(c Config) (*Store, error) {
	client, err := minio.New(c.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(c.AccessKey, c.SecretKey, ""),
		Secure: c.Secure,
		Region: c.Region,
	})
	if err != nil {
		return nil, err
	}
	// EncodeAll of one encoder is safe for concurrent use
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
	if err != nil {
		return nil, err
	}
	return &Store{client: client, bucket: c.Bucket, prefix: c.Prefix, encoder: encoder}, nil
}

// Put stores the answer under <prefix><task id>/<random>: attempts of a task do not overwrite each other,
// a repeat of Complete carries the same reference.
func (s *Store) Put(ctx context.Context, taskID string, data []byte) (domain.BlobRef, error) {
	key := s.prefix + taskID + "/" + rand.Text()
	ref, body := s.encode(data)
	ref.Location = "s3://" + s.bucket + "/" + key
	_, err := s.client.PutObject(ctx, s.bucket, key, bytes.NewReader(body), int64(len(body)),
		minio.PutObjectOptions{ContentType: "application/zstd"})
	if err != nil {
		return domain.BlobRef{}, err
	}
	return ref, nil
}

// encode - the reference without a location and the body of the object.
func (s *Store) encode(data []byte) (domain.BlobRef, []byte) {
	sum := sha256.Sum256(data)
	ref := domain.BlobRef{Size: int64(len(data)), Sha256: hex.EncodeToString(sum[:]), Encoding: Encoding}
	return ref, s.encoder.EncodeAll(data, make([]byte, 0, len(data)/4))
}
