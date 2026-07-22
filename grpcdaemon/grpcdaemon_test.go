package grpcdaemon_test

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/seeruk/go-daemon"
	"github.com/seeruk/go-daemon/grpcdaemon"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"

	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func TestRoutine_Initialize(t *testing.T) {
	t.Run("should panic if initialized with a nil server", func(t *testing.T) {
		routine := grpcdaemon.NewRoutine(nil, "")

		assert.PanicsWithValue(t, "grpcdaemon: nil server", func() {
			_ = routine.Initialize(context.Background())
		})
	})

	t.Run("should return an error if the address is already in use", func(t *testing.T) {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		t.Cleanup(func() { _ = ln.Close() })

		routine := grpcdaemon.NewRoutine(grpc.NewServer(), ln.Addr().String())
		assert.Error(t, routine.Initialize(context.Background()))
	})
}

func TestRoutine_Run(t *testing.T) {
	t.Run("should panic if run before initialization", func(t *testing.T) {
		routine := grpcdaemon.NewRoutine(grpc.NewServer(), "")

		assert.PanicsWithValue(t, "grpcdaemon: routine not initialized", func() {
			_ = routine.Run(context.Background())
		})
	})

	t.Run("should panic if run without a force shutdown context", func(t *testing.T) {
		routine := grpcdaemon.NewRoutine(grpc.NewServer(), "127.0.0.1:0")

		err := routine.Initialize(context.Background())
		require.NoError(t, err)

		assert.PanicsWithValue(t, "grpcdaemon: force shutdown context not set on context", func() {
			_ = routine.Run(context.Background())
		})
	})

	t.Run("should serve gRPC and call lifecycle hooks", func(t *testing.T) {
		served := make(chan net.Addr, 1)
		stopped := make(chan error, 1)
		stop := make(chan struct{})
		stopErr := errors.New("stop")

		server := grpc.NewServer()
		healthpb.RegisterHealthServer(server, health.NewServer())

		routine := grpcdaemon.NewRoutine(server, "127.0.0.1:0",
			grpcdaemon.OnServe(func(ln net.Listener, _ *grpc.Server) {
				served <- ln.Addr()
			}),
			grpcdaemon.OnStop(func(_ net.Listener, _ *grpc.Server, err error) {
				stopped <- err
			}),
		)

		errCh := runUntilStopped(time.Second, routine, stop, stopErr)

		conn := newClient(t, receive(t, served))
		client := healthpb.NewHealthClient(conn)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		resp, err := client.Check(ctx, &healthpb.HealthCheckRequest{})
		require.NoError(t, err)
		assert.Equal(t, healthpb.HealthCheckResponse_SERVING, resp.Status)

		close(stop)
		assert.ErrorIs(t, receive(t, errCh), stopErr)
		assert.ErrorIs(t, receive(t, stopped), context.Canceled)
	})

	t.Run("should return ErrAlreadyStopped if the server was stopped before the routine starts", func(t *testing.T) {
		server := grpc.NewServer()
		server.Stop()

		routine := grpcdaemon.NewRoutine(server, "127.0.0.1:0")
		assert.ErrorIs(t, daemon.RunE(time.Second, routine), daemon.ErrAlreadyStopped)
	})

	t.Run("should return serving errors", func(t *testing.T) {
		routine := grpcdaemon.NewRoutine(grpc.NewServer(), "127.0.0.1:0",
			grpcdaemon.OnServe(func(ln net.Listener, _ *grpc.Server) {
				_ = ln.Close()
			}),
		)

		err := daemon.RunE(time.Second, routine)
		assert.Error(t, err)
		assert.NotErrorIs(t, err, daemon.ErrAlreadyStopped)
	})

	t.Run("should force close requests after the grace period", func(t *testing.T) {
		requestStarted := make(chan struct{})
		releaseRequest := make(chan struct{})
		defer close(releaseRequest)
		served := make(chan net.Addr, 1)
		stopped := make(chan error, 1)
		stop := make(chan struct{})
		stopErr := errors.New("stop")

		server := grpc.NewServer()
		healthpb.RegisterHealthServer(server, &blockingHealthServer{
			requestStarted: requestStarted,
			releaseRequest: releaseRequest,
		})
		routine := grpcdaemon.NewRoutine(server, "127.0.0.1:0",
			grpcdaemon.OnServe(func(ln net.Listener, _ *grpc.Server) {
				served <- ln.Addr()
			}),
			grpcdaemon.OnStop(func(_ net.Listener, _ *grpc.Server, err error) {
				stopped <- err
			}),
		)

		errCh := runUntilStopped(20*time.Millisecond, routine, stop, stopErr)
		conn := newClient(t, receive(t, served))
		client := healthpb.NewHealthClient(conn)
		requestErrCh := make(chan error, 1)
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_, err := client.Check(ctx, &healthpb.HealthCheckRequest{})
			requestErrCh <- err
		}()
		receive(t, requestStarted)

		close(stop)
		runErr := receive(t, errCh)
		assert.ErrorIs(t, runErr, daemon.ErrGracePeriodExceeded)
		assert.ErrorIs(t, runErr, stopErr)
		assert.ErrorIs(t, receive(t, stopped), context.Canceled)
		assert.Error(t, receive(t, requestErrCh))
	})
}

type blockingHealthServer struct {
	healthpb.UnimplementedHealthServer
	requestStarted chan<- struct{}
	releaseRequest <-chan struct{}
}

func (s *blockingHealthServer) Check(
	ctx context.Context,
	_ *healthpb.HealthCheckRequest,
) (*healthpb.HealthCheckResponse, error) {
	close(s.requestStarted)
	select {
	case <-s.releaseRequest:
		return &healthpb.HealthCheckResponse{}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func newClient(t *testing.T, addr net.Addr) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient(addr.String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func runUntilStopped(
	gracePeriod time.Duration,
	routine daemon.Routine,
	stop <-chan struct{},
	stopErr error,
) <-chan error {
	errCh := make(chan error, 1)
	go func() {
		errCh <- daemon.RunE(gracePeriod,
			routine,
			daemon.RoutineFunc(func(context.Context) error {
				<-stop
				return stopErr
			}),
		)
	}()
	return errCh
}

func receive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(time.Second):
		require.FailNow(t, "timed out waiting for channel")
		var zero T
		return zero
	}
}
