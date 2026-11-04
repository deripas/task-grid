package config

import (
	"strings"
	"testing"
	"time"
)

func required(t *testing.T) {
	t.Setenv("KIND", "echo")
	t.Setenv("HANDLER_METHOD", "/echo.v1.Echo/Echo")
}

func TestDefaults(t *testing.T) {
	required(t)
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.NodeAddress != "localhost:9090" || c.Sidecar.Capacity != 8 || c.Sidecar.WorkerID == "" ||
		c.Sidecar.RetryInterval != time.Second || c.Sidecar.DrainTimeout != time.Minute ||
		c.Handler.Target != "localhost:50051" || c.Handler.Timeout != 10*time.Minute ||
		c.Handler.MaxResponse != 100<<20 || c.Sidecar.InlineMax != 512<<10 ||
		c.Blob.Endpoint != "" || c.Blob.Bucket != "results" || c.Blob.Prefix != "" || c.Blob.Secure {
		t.Fatalf("defaults: %+v", c)
	}
}

func TestRequired(t *testing.T) {
	for _, name := range []string{"KIND", "HANDLER_METHOD"} {
		t.Run(name, func(t *testing.T) {
			required(t)
			t.Setenv(name, "")
			if _, err := Load(); err == nil || !strings.Contains(err.Error(), name) {
				t.Fatalf("err %v, want %s error", err, name)
			}
		})
	}
}

// 0 or negative interval - a loop without a pause; CAPACITY and MAX_RESPONSE must be positive; an inline result
// above 4MiB would not be accepted by the node.
func TestValuesValidated(t *testing.T) {
	cases := map[string][]string{
		"RETRY_INTERVAL":  {"0", "-1s", "soon"},
		"DRAIN_TIMEOUT":   {"0s"},
		"HANDLER_TIMEOUT": {"-1m"},
		"CAPACITY":        {"0", "x"},
		"MAX_RESPONSE":    {"0", "-1", "4MB"},
		"INLINE_MAX":      {"5MiB", "x"},
		"BLOB_S3_SECURE":  {"maybe"},
	}
	for name, values := range cases {
		for _, v := range values {
			t.Run(name+"="+v, func(t *testing.T) {
				required(t)
				t.Setenv(name, v)
				if _, err := Load(); err == nil || !strings.Contains(err.Error(), name) {
					t.Fatalf("err %v, want %s error", err, name)
				}
			})
		}
	}
}

func TestSizes(t *testing.T) {
	required(t)
	t.Setenv("MAX_RESPONSE", "1GiB")
	t.Setenv("INLINE_MAX", "0")
	if c, err := Load(); err != nil || c.Handler.MaxResponse != 1<<30 || c.Sidecar.InlineMax != 0 {
		t.Fatalf("MAX_RESPONSE=1GiB, INLINE_MAX=0: %v, %v, %v", c.Handler.MaxResponse, c.Sidecar.InlineMax, err)
	}
}

func TestInlineMaxAboveMaxResponse(t *testing.T) {
	required(t)
	t.Setenv("MAX_RESPONSE", "256KiB")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "INLINE_MAX") {
		t.Fatalf("err %v, want INLINE_MAX error", err)
	}
}
