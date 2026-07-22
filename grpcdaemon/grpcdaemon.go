package grpcdaemon

import (
	"context"
	"errors"
	"net"

	"github.com/seeruk/go-daemon"
	"google.golang.org/grpc"
)

// Option is a function that is used to configure a gRPC server Routine.
type Option func(*Routine)

// OnServe sets the callback to be called when the server is starting to listen. The callback will
// be called with the listener and server, so information can be pulled from them.
func OnServe(cb func(net.Listener, *grpc.Server)) Option {
	return func(r *Routine) {
		r.onServe = cb
	}
}

// OnStop sets the callback to be called when the server has stopped. The callback will be called
// with the listener, server, and error that caused the server to stop.
func OnStop(cb func(net.Listener, *grpc.Server, error)) Option {
	return func(r *Routine) {
		r.onStop = cb
	}
}

type Routine struct {
	listener net.Listener
	server   *grpc.Server
	addr     string

	onServe func(net.Listener, *grpc.Server)
	onStop  func(net.Listener, *grpc.Server, error)
}

// NewRoutine returns a new Routine instance.
func NewRoutine(server *grpc.Server, addr string, opts ...Option) *Routine {
	r := &Routine{
		server: server,
		addr:   addr,
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

func (r *Routine) Initialize(ctx context.Context) error {
	if r.server == nil {
		panic("grpcdaemon: nil server")
	}

	addr := r.addr
	if addr == "" {
		addr = ":50051"
	}

	var lc net.ListenConfig

	listener, err := lc.Listen(ctx, "tcp", addr)
	if err != nil {
		return err
	}

	r.listener = listener
	return nil
}

func (r *Routine) Run(ctx context.Context) (err error) {
	if r.listener == nil {
		panic("grpcdaemon: routine not initialized")
	}

	forceCtx, ok := daemon.ForceShutdownContextFromContext(ctx)
	if !ok {
		panic("grpcdaemon: force shutdown context not set on context")
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
		errCh <- r.server.Serve(r.listener)
	}()

	select {
	case err = <-errCh:
		if err == nil || errors.Is(err, grpc.ErrServerStopped) {
			return daemon.ErrAlreadyStopped
		}
		return err

	case <-ctx.Done():
		shutdownCh := make(chan struct{}, 1)
		go func() {
			r.server.GracefulStop()
			// In-flight requests can block this forever, depending on how the server is configured.
			// We call GracefulStop in a goroutine so that we can force a close, as signalled by the
			// "force shutdown context" provided on the context passed to Run.
			shutdownCh <- struct{}{}
		}()

		select {
		case <-shutdownCh:
			return shutdownError(ctx, <-errCh)

		case <-forceCtx.Done():
			r.server.Stop()
			return shutdownError(ctx, <-errCh)
		}
	}
}

func shutdownError(ctx context.Context, errs ...error) error {
	var out error
	for _, err := range errs {
		if err == nil || errors.Is(err, grpc.ErrServerStopped) {
			continue
		}
		out = errors.Join(out, err)
	}
	if out != nil {
		return out
	}
	return ctx.Err()
}
