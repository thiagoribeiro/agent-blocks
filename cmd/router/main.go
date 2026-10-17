// Command router runs the Agent Blocks AppService router.
package main

import (
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/thiagoribeiro/agent-blocks/internal/router"
)

func main() {
	cfg, err := router.LoadConfig()
	if err != nil {
		slog.Error("config error", "error", err)
		os.Exit(1)
	}
	srv := http.Server{Addr: fmt.Sprintf(":%d", cfg.Port), Handler: router.NewMux(cfg), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second, WriteTimeout: 60 * time.Second, IdleTimeout: 60 * time.Second}
	slog.Info("agent-blocks starting", "port", cfg.Port)
	if err = srv.ListenAndServe(); err != nil {
		slog.Error("server stopped", "error", err)
		os.Exit(1)
	}
}
