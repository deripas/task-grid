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
		c.Handler.Target != "localhost:50051" || c.Handler.Timeout != 10*time.Minute || c.Handler.MaxResponse != 4<<20 {
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

// 0 or negative interval - a loop without a pause; CAPACITY and MAX_RESULT must be positive.
func TestValuesValidated(t *testing.T) {
	cases := map[string][]string{
		"RETRY_INTERVAL":  {"0", "-1s", "soon"},
		"DRAIN_TIMEOUT":   {"0s"},
		"HANDLER_TIMEOUT": {"-1m"},
		"CAPACITY":        {"0", "x"},
		"MAX_RESULT":      {"0", "-1", "4MB"},
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
	t.Setenv("MAX_RESULT", "512KiB")
	if c, err := Load(); err != nil || c.Handler.MaxResponse != 512<<10 {
		t.Fatalf("MAX_RESULT=512KiB: %v, %v", c.Handler.MaxResponse, err)
	}
}
