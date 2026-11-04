// Format of the objects. Put against a real bucket - SidecarCompatibilityIT of task-grid-worker-remote
// (SeaweedFS in Testcontainers, `make it` in the root).
package blob

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/klauspost/compress/zstd"
)

func TestEncodeCompressesAndDescribesTheAnswer(t *testing.T) {
	s, err := New(Config{Endpoint: "localhost:8333", Bucket: "results"})
	if err != nil {
		t.Fatal(err)
	}
	data := bytes.Repeat([]byte("<url><loc>https://example.com/</loc></url>\n"), 1<<14)
	sum := sha256.Sum256(data)

	ref, body := s.encode(data)

	if ref.Size != int64(len(data)) || ref.Sha256 != hex.EncodeToString(sum[:]) || ref.Encoding != "zstd" {
		t.Fatalf("ref %+v", ref)
	}
	if len(body) >= len(data)/10 {
		t.Fatalf("%d bytes -> %d, want compressed", len(data), len(body))
	}
	dec, err := zstd.NewReader(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer dec.Close()
	got, err := dec.DecodeAll(body, nil)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("decoded %d bytes, err %v", len(got), err)
	}
}
