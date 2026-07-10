package httpsrv

import (
	"context"
	"errors"
	"net"
	"net/http"

	"github.com/seeruk/go-daemon"
)

// Option is a function that is used to configure an HTTP server Routine.
type Option func(*Routine)

// OnServe sets the callback to be called when the server is starting to listen. The callback will
// be called with the listener and server, so information can be pulled from them.
func OnServe(cb func(net.Listener, *http.Server)) Option {
	return func(r *Routine) {
		r.onServe = cb
	}
}

// OnStop sets the callback to be called when the server has stopped. The callback will be called
// with the listener, server, and error that caused the server to stop.
func OnStop(cb func(net.Listener, *http.Server, error)) Option {
	return func(r *Routine) {
		r.onStop = cb
	}
}

// Routine is a routine for building HTTP server daemons. For simple servers, you can pass in your
// configured http.Server instance, and it will handle appropriately starting and gracefully
// stopping the server as needed.
type Routine struct {
	listener net.Listener
	server   *http.Server
	serve    func(net.Listener, *http.Server) error

	onServe func(net.Listener, *http.Server)
	onStop  func(net.Listener, *http.Server, error)
}

// NewRoutine returns a new Routine instance for an HTTP server without TLS support.
func NewRoutine(server *http.Server, opts ...Option) *Routine {
	r := &Routine{
		server: server,
		serve: func(l net.Listener, s *http.Server) error {
			return s.Serve(l)
		},
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// NewRoutineWithTLS returns a new Routine instance for an HTTP server with TLS support.
func NewRoutineWithTLS(server *http.Server, certFile, keyFile string, opts ...Option) *Routine {
	r := &Routine{
		server: server,
		serve: func(l net.Listener, s *http.Server) error {
			return s.ServeTLS(l, certFile, keyFile)
		},
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// Initialize attempts to bind to the address requested on the server provided to this routine.
func (r *Routine) Initialize(ctx context.Context) error {
	if r.server == nil {
		panic("httpsrv: nil server")
	}

	addr := r.server.Addr
	if addr == "" {
		addr = ":http"
	}

	var lc net.ListenConfig

	listener, err := lc.Listen(ctx, "tcp", addr)
	if err != nil {
		return err
	}

	r.listener = listener
	return nil
}

// Run attempts to start serving connections.
func (r *Routine) Run(ctx context.Context) (err error) {
	if r.listener == nil {
		panic("httpsrv: routine not initialized")
	}

	forceCtx, ok := daemon.ForceShutdownContextFromContext(ctx)
	if !ok {
		panic("httpsrv: force shutdown context not set on context")
	}

	if r.onServe != nil {
		r.onServe(r.listener, r.server)
	}

	defer func() {
		if r.onStop != nil {
			r.onStop(r.listener, r.server, err)
		}
	}()

	errCh := make(chan error, 1)
	go func() {
		errCh <- r.serve(r.listener, r.server)
	}()

	select {
	case err = <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return daemon.ErrAlreadyStopped
		}
		return err

	case <-ctx.Done():
		shutdownErrCh := make(chan error, 1)
		go func() {
			// In-flight requests can block this forever, depending on how the server / it's
			// handlers are configured. We call it in a goroutine so that we can force a close, as
			// signalled by the "force shutdown context" provided on the context passed to Run.
			shutdownErrCh <- r.server.Shutdown(context.Background())
		}()

		select {
		case err = <-shutdownErrCh:
			return shutdownError(ctx, err, <-errCh)

		case <-forceCtx.Done():
			closeErr := r.server.Close()
			return shutdownError(ctx, closeErr, <-shutdownErrCh, <-errCh)
		}
	}
}

func shutdownError(ctx context.Context, errs ...error) error {
	var out error
	for _, err := range errs {
		if err == nil || errors.Is(err, http.ErrServerClosed) {
			continue
		}
		out = errors.Join(out, err)
	}
	if out != nil {
		return out
	}
	return ctx.Err()
}
