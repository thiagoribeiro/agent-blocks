package main

import (
	"github.com/thiagoribeiro/agent-blocks/internal/roomadmin"
	"log"
	"net/http"
	"os"
	"time"
)

func main() {
	for _, k := range []string{"NIMLET_ADMIN_DB_DSN", "NIMLET_NAMESPACE", "ROOM_ADMIN_TOKEN", "NIMLET_URL", "AS_TOKEN", "NIMLET_CONSUMER_TOKEN", "NIMLET_BOT_USERNAME"} {
		if os.Getenv(k) == "" {
			log.Fatal("missing configuration: " + k)
		}
	}
	binary := os.Getenv("NIMLET_BINARY")
	if binary == "" {
		binary = "/usr/local/bin/nimlet"
	}
	s := &roomadmin.Server{Namespace: os.Getenv("NIMLET_NAMESPACE"), Token: os.Getenv("ROOM_ADMIN_TOKEN"), NimletURL: os.Getenv("NIMLET_URL"), ASToken: os.Getenv("AS_TOKEN"), BotToken: os.Getenv("NIMLET_CONSUMER_TOKEN"), BotUsername: os.Getenv("NIMLET_BOT_USERNAME"), CLI: roomadmin.Executable(binary, os.Getenv("NIMLET_NAMESPACE")), HTTP: &http.Client{Timeout: 20 * time.Second}}
	addr := os.Getenv("ROOM_ADMIN_ADDR")
	if addr == "" {
		addr = ":4020"
	}
	srv := &http.Server{Addr: addr, Handler: s, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 35 * time.Second, WriteTimeout: 35 * time.Second}
	log.Fatal(srv.ListenAndServe())
}
