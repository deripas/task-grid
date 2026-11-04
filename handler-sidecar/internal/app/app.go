// Package app assembles the sidecar: settings -> adapters (node, handler) -> core (sidecar), stop signals.
// The only package that knows all the others.
package app

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/deripas/task-grid/handler-sidecar/internal/config"
	"github.com/deripas/task-grid/handler-sidecar/internal/handler"
	"github.com/deripas/task-grid/handler-sidecar/internal/node"
	"github.com/deripas/task-grid/handler-sidecar/internal/sidecar"
)

// Run works until SIGTERM / Ctrl+C and returns the exit code of the process.
func Run() int {
	level := slog.LevelInfo
	if os.Getenv("LOG_LEVEL") == "debug" {
		level = slog.LevelDebug
	}
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level}))

	cfg, err := config.Load()
	if err != nil {
		log.Error("config", "err", err)
		return 2
	}
	h, err := handler.New(cfg.Handler)
	if err != nil {
		log.Error("handler client", "err", err)
		return 1
	}
	defer h.Close()
	n, err := node.New(cfg.NodeAddress)
	if err != nil {
		log.Error("node client", "err", err)
		return 1
	}
	defer n.Close()

	s := sidecar.New(cfg.Sidecar, n, h, log)
	defer s.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	log.Info("started", "node", cfg.NodeAddress, "handler", cfg.Handler.Target, "method", cfg.Handler.Method)
	if err := s.Run(ctx); err != nil {
		// the orchestrator and monitoring must see that the stop was not clean: results are lost
		log.Error("stopped", "err", err)
		return 1
	}
	return 0
}
