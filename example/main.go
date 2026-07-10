package main

import (
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/seeruk/go-daemon"
	"github.com/seeruk/go-daemon/httpsrv"
)

func main() {
	os.Exit(daemon.Run(time.Second,
		httpServerRoutine(),
	))
}

func httpServerRoutine() *httpsrv.Routine {
	server := &http.Server{
		Addr: ":0",
	}

	return httpsrv.NewRoutine(server,
		httpsrv.OnServe(func(listener net.Listener, server *http.Server) {
			slog.Info("http server started", "addr", listener.Addr().String())
		}),
		httpsrv.OnStop(func(listener net.Listener, server *http.Server, err error) {
			slog.Info("http server stopped")
		}),
	)
}
