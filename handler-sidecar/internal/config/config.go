// Package config reads the settings of the sidecar from the environment (see README) and lays them out into
// the settings of the packages: fields and their meaning live in sidecar.Config, handler.Config and blob.Config;
// here are only
// names of the variables, defaults and checks.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/deripas/task-grid/handler-sidecar/internal/blob"
	"github.com/deripas/task-grid/handler-sidecar/internal/handler"
	"github.com/deripas/task-grid/handler-sidecar/internal/sidecar"
)

// Config - settings of the sidecar from the environment.
type Config struct {
	NodeAddress string // host:port of the task-grid node
	Sidecar     sidecar.Config
	Handler     handler.Config
	Blob        blob.Config // Endpoint empty - no blob storage
}

// maxInline - default limit of an inbound message of the Java gRPC server: a larger inline result would not be
// accepted by the node.
const maxInline = 4 << 20

// Load reads the settings from the environment; an error - an invalid value or a missing required one.
func Load() (Config, error) {
	host, _ := os.Hostname()
	c := Config{
		NodeAddress: env("NODE_ADDRESS", "localhost:9090"),
		Sidecar: sidecar.Config{
			WorkerID: env("WORKER_ID", host),
			Kind:     env("KIND", ""),
		},
		Handler: handler.Config{
			Target: env("HANDLER_TARGET", "localhost:50051"),
			Method: env("HANDLER_METHOD", ""),
		},
		Blob: blob.Config{
			Endpoint:  env("BLOB_S3_ENDPOINT", ""),
			Bucket:    env("BLOB_S3_BUCKET", "results"),
			AccessKey: env("BLOB_S3_ACCESS_KEY", ""),
			SecretKey: env("BLOB_S3_SECRET_KEY", ""),
			Region:    env("BLOB_S3_REGION", ""),
			Prefix:    env("BLOB_PREFIX", ""),
		},
	}
	if c.Sidecar.Kind == "" {
		return c, fmt.Errorf("KIND is required, e.g. fetch")
	}
	if c.Handler.Method == "" {
		return c, fmt.Errorf("HANDLER_METHOD is required, e.g. /crawler.fetcher.v1.Fetcher/Fetch")
	}
	if c.Sidecar.WorkerID == "" {
		return c, fmt.Errorf("WORKER_ID is required: hostname is not available")
	}
	var err error
	if c.Sidecar.Capacity, err = strconv.Atoi(env("CAPACITY", "8")); err != nil || c.Sidecar.Capacity < 1 {
		return c, fmt.Errorf("CAPACITY: %q, want >= 1", env("CAPACITY", ""))
	}
	for _, d := range []struct {
		name, def string
		to        *time.Duration
	}{
		{"HANDLER_TIMEOUT", "10m", &c.Handler.Timeout},
		{"RETRY_INTERVAL", "1s", &c.Sidecar.RetryInterval},
		{"DRAIN_TIMEOUT", "60s", &c.Sidecar.DrainTimeout},
	} {
		if *d.to, err = time.ParseDuration(env(d.name, d.def)); err != nil {
			return c, fmt.Errorf("%s: %w", d.name, err)
		}
		// 0 for a retry interval is a loop without a pause
		if *d.to <= 0 {
			return c, fmt.Errorf("%s: %v, want > 0", d.name, *d.to)
		}
	}
	if c.Handler.MaxResponse, err = parseSize(env("MAX_RESPONSE", "100MiB")); err != nil || c.Handler.MaxResponse == 0 {
		return c, fmt.Errorf("MAX_RESPONSE: %q, want > 0", env("MAX_RESPONSE", ""))
	}
	c.Sidecar.InlineMax, err = parseSize(env("INLINE_MAX", "512KiB"))
	if err != nil || c.Sidecar.InlineMax > maxInline || c.Sidecar.InlineMax > c.Handler.MaxResponse {
		return c, fmt.Errorf("INLINE_MAX: %q, want <= 4MiB and <= MAX_RESPONSE", env("INLINE_MAX", ""))
	}
	if c.Blob.Secure, err = strconv.ParseBool(env("BLOB_S3_SECURE", "false")); err != nil {
		return c, fmt.Errorf("BLOB_S3_SECURE: %w", err)
	}
	return c, nil
}

// parseSize: "524288", "512KiB", "4MiB", "1GiB".
func parseSize(s string) (int, error) {
	mult := 1
	for suffix, m := range map[string]int{"KiB": 1 << 10, "MiB": 1 << 20, "GiB": 1 << 30} {
		if strings.HasSuffix(s, suffix) {
			s, mult = strings.TrimSuffix(s, suffix), m
			break
		}
	}
	n, err := strconv.Atoi(strings.TrimSpace(s))
	if err != nil || n < 0 {
		return 0, fmt.Errorf("bad size %q", s)
	}
	return n * mult, nil
}

func env(name, def string) string {
	if v := strings.TrimSpace(os.Getenv(name)); v != "" {
		return v
	}
	return def
}
