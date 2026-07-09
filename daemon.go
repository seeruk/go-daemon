package daemon

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"
)

var (
	ErrGracePeriodExceeded = errors.New("daemon: grace period exceeded")
	ErrInterrupted         = errors.New("daemon: interrupted")
)

// Run is a helper around RunE which returns an int expected to be used as an exit code passed to
// os.Exit.
func Run(gracePeriod time.Duration, routines ...Routine) int {
	if err := RunE(gracePeriod, routines...); err != nil {
		return 1
	}
	return 0
}

// RunE starts the given routines, blocking until they are terminated, or error.
//
// If the process receives an interrupt signal, the context passed to all routines will be cancelled
// to signal for them to begin gracefully shutting down. If no further signals are received, but the
// routines don't terminate within the provided grace period, an ErrGracePeriodExceeded error will
// be returned. If a second signal is received RunE will attempt to return immediately, returning an
// ErrInterrupted. In this scenario, if the program doesn't exit, some resources may leak.
//
// The first error encountered is returned, except for context.Canceled errors.
//
// Routines are started in the order they're given. If a Routine is an InitializableRoutine then
// its Initialize method will be called before Run, and before moving onto starting the next
// Routine. If any Routine fails to initialize, the rest of the routines will not be started.
func RunE(gracePeriod time.Duration, routines ...Routine) error {
	if len(routines) == 0 {
		panic("daemon: no routines provided")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 2)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	errCh := make(chan error, len(routines))

	go func() {
		defer close(errCh)

		g, ctx := errgroup.WithContext(ctx)

		var initErr error
		for _, routine := range routines {
			if initializable, ok := routine.(InitializableRoutine); ok {
				if err := initializable.Initialize(ctx); err != nil {
					initErr = err
					cancel()
					break
				}
			}

			g.Go(func() error {
				return routine.Run(ctx)
			})
		}

		err := g.Wait()

		// For the following error checks, we don't propagate context.Canceled because it's expected to
		// be cancelled. It might've been cancelled by the previous routing failing to initialize too.

		// If we got an initialization error, we'll report that over anything else, as it's probably the
		// most relevant one to see.
		if initErr != nil && !errors.Is(initErr, context.Canceled) {
			errCh <- initErr
			return
		}

		// Otherwise, we check if there was an error in-flight with any of the routines.
		if err != nil && !errors.Is(err, context.Canceled) {
			errCh <- err
			return
		}
	}()

	select {
	case err := <-errCh:
		return err
	case <-sigCh:
		cancel()
	}

	select {
	case err := <-errCh:
		return err
	case <-time.After(gracePeriod):
		return ErrGracePeriodExceeded
	case <-sigCh:
		return ErrInterrupted
	}
}
