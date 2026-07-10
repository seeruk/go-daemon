package httpsrv_test

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/seeruk/go-daemon"
	"github.com/seeruk/go-daemon/httpsrv"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRoutine_Initialize(t *testing.T) {
	t.Run("should panic if initialized with a nil server", func(t *testing.T) {
		routine := httpsrv.NewRoutine(nil)

		assert.PanicsWithValue(t, "httpsrv: nil server", func() {
			_ = routine.Initialize(context.Background())
		})
	})

	t.Run("should return an error if the address is already in use", func(t *testing.T) {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		t.Cleanup(func() { _ = ln.Close() })

		routine := httpsrv.NewRoutine(&http.Server{Addr: ln.Addr().String()})
		assert.Error(t, routine.Initialize(context.Background()))
	})
}

func TestRoutine_Run(t *testing.T) {
	t.Run("should panic if run before initialization", func(t *testing.T) {
		routine := httpsrv.NewRoutine(&http.Server{})

		assert.PanicsWithValue(t, "httpsrv: routine not initialized", func() {
			_ = routine.Run(context.Background())
		})
	})

	t.Run("should panic if run without a force shutdown context", func(t *testing.T) {
		routine := httpsrv.NewRoutine(&http.Server{})

		err := routine.Initialize(context.Background())
		require.NoError(t, err)

		assert.PanicsWithValue(t, "httpsrv: force shutdown context not set on context", func() {
			_ = routine.Run(context.Background())
		})
	})

	t.Run("should serve HTTP and call lifecycle hooks", func(t *testing.T) {
		served := make(chan net.Addr, 1)
		stopped := make(chan error, 1)
		stop := make(chan struct{})
		stopErr := errors.New("stop")

		server := &http.Server{
			Addr: "127.0.0.1:0",
			Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = io.WriteString(w, "hello")
			}),
		}

		routine := httpsrv.NewRoutine(server,
			httpsrv.OnServe(func(ln net.Listener, _ *http.Server) {
				served <- ln.Addr()
			}),
			httpsrv.OnStop(func(_ net.Listener, _ *http.Server, err error) {
				stopped <- err
			}),
		)

		errCh := runUntilStopped(time.Second, routine, stop, stopErr)
		addr := receive(t, served)

		client := &http.Client{Timeout: time.Second}
		resp, err := client.Get("http://" + addr.String())
		require.NoError(t, err)

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())

		assert.Equal(t, http.StatusOK, resp.StatusCode)
		assert.Equal(t, "hello", string(body))

		close(stop)
		assert.ErrorIs(t, receive(t, errCh), stopErr)
		assert.ErrorIs(t, receive(t, stopped), context.Canceled)
	})

	t.Run("should return ErrAlreadyStopped if the server was stopped before the routine starts", func(t *testing.T) {
		stop := make(chan struct{})

		server := &http.Server{Addr: ":0"}

		err := server.Close()
		require.NoError(t, err)

		routine := httpsrv.NewRoutine(server)

		errCh := runUntilStopped(time.Second, routine, stop, nil)

		close(stop)
		assert.ErrorIs(t, receive(t, errCh), daemon.ErrAlreadyStopped)
	})

	t.Run("should return TLS configuration errors", func(t *testing.T) {
		dir := t.TempDir()

		listenerCh := make(chan net.Listener, 1)
		routine := httpsrv.NewRoutineWithTLS(
			&http.Server{Addr: "127.0.0.1:0"},
			dir+"/cert.pem",
			dir+"/key.pem",
			httpsrv.OnServe(func(ln net.Listener, _ *http.Server) {
				listenerCh <- ln
			}),
		)

		err := daemon.RunE(time.Second, routine)

		listener := receive(t, listenerCh)
		t.Cleanup(func() { _ = listener.Close() })

		assert.ErrorContains(t, err, "cert.pem")
	})

	t.Run("should force close requests after the grace period", func(t *testing.T) {
		requestStarted := make(chan struct{})
		releaseRequest := make(chan struct{})
		defer close(releaseRequest)
		served := make(chan net.Addr, 1)
		stopped := make(chan error, 1)
		stop := make(chan struct{})
		stopErr := errors.New("stop")

		server := &http.Server{
			Addr: "127.0.0.1:0",
			Handler: http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
				close(requestStarted)
				<-releaseRequest
			}),
		}
		routine := httpsrv.NewRoutine(server,
			httpsrv.OnServe(func(ln net.Listener, _ *http.Server) {
				served <- ln.Addr()
			}),
			httpsrv.OnStop(func(_ net.Listener, _ *http.Server, err error) {
				stopped <- err
			}),
		)

		errCh := runUntilStopped(20*time.Millisecond, routine, stop, stopErr)
		addr := receive(t, served)
		requestErrCh := make(chan error, 1)
		go func() {
			client := &http.Client{Timeout: time.Second}
			resp, err := client.Get("http://" + addr.String())
			if resp != nil {
				_ = resp.Body.Close()
			}
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
