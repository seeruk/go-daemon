package main

import (
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/seeruk/go-daemon"
	"github.com/seeruk/go-daemon/httpdaemon"
)

func main() {
	os.Exit(daemon.Run(time.Second,
		httpServerRoutine(),
	))
}

func httpServerRoutine() *httpdaemon.Routine {
	server := &http.Server{
		Addr: ":0",
	}

	return httpdaemon.NewRoutine(server,
		httpdaemon.OnServe(func(listener net.Listener, server *http.Server) {
			slog.Info("http server started", "addr", listener.Addr().String())
		}),
		httpdaemon.OnStop(func(listener net.Listener, server *http.Server, err error) {
			slog.Info("http server stopped")
		}),
	)
}
