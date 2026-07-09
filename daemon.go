package daemon

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"
)

var (
	ErrCompleted           = errors.New("daemon: routine completed")
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
	sigCh := make(chan os.Signal, 2)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	return run(sigCh, gracePeriod, routines...)
}

// run is the internal orchestration implementation for running routines.
func run(sigCh <-chan os.Signal, gracePeriod time.Duration, routines ...Routine) error {
	if len(routines) == 0 {
		panic("daemon: no routines provided")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stopCh := make(chan error, 1)
	doneCh := make(chan error, 1)

	once := sync.Once{}

	go func() {
		g, ctx := errgroup.WithContext(ctx)

		for _, routine := range routines {
			if initializable, ok := routine.(InitializableRoutine); ok {
				if err := initializable.Initialize(ctx); err != nil {
					sendSignificantError(ctx, err, &once, stopCh)
					cancel() // Cancel the other routines if one fails to initialize
					break
				}
			}

			g.Go(func() error {
				if err := routine.Run(ctx); err != nil {
					// We allow ErrCompleted to signal that this routine has exited cleanly and
					// shouldn't cancel the other routines.
					if errors.Is(err, ErrCompleted) {
						return nil
					}

					sendSignificantError(ctx, err, &once, stopCh)
					cancel() // Cancel the other routines if one errs in-flight
					return err
				}
				return nil
			})
		}

		doneCh <- significantError(ctx, g.Wait())
	}()

	var stopErr error

	select {
	case stopErr = <-stopCh:
	case <-sigCh:
	case err := <-doneCh:
		// If we didn't get a signal, and we aren't stopping because there was an error (from
		// stopCh), then all the routines must have simply finished.
		return err
	}

	cancel()

	select {
	case err := <-doneCh:
		// We always try to wait for the routines to stop, if we got into this case, then the
		// routines did all stop within the grace period. If we see a stopErr, it means an error
		// actually occurred either during initialization, or after a routine had started.
		if stopErr != nil {
			return stopErr
		}
		return err
	case <-time.After(gracePeriod):
		return errors.Join(ErrGracePeriodExceeded, stopErr)
	case <-sigCh:
		return errors.Join(ErrInterrupted, stopErr)
	}
}

func sendSignificantError(ctx context.Context, err error, once *sync.Once, stopCh chan error) {
	if stopErr := significantError(ctx, err); stopErr != nil {
		once.Do(func() {
			stopCh <- stopErr
		})
	}
}

func significantError(ctx context.Context, err error) error {
	// We don't want to return a context.Canceled if our context was the one that produced it, but
	// if it came from within a routine, we should still keep that.
	if errors.Is(err, context.Canceled) && ctx.Err() != nil {
		return nil
	}
	return err
}
