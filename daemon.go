package daemon

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"time"

	"golang.org/x/sync/errgroup"
)

// Run is a helper around RunE which returns an int expected to be used as an exit code passed to
// os.Exit.
func Run(gracePeriod time.Duration, routines ...Routine) int {
	if err := RunE(gracePeriod, routines...); err != nil {
		fmt.Println(err)
		return 1
	}
	return 0
}

// RunE starts the given routines, blocking until they are terminated, or error.
// If the process receives an interrupt signal, the context passed to all routines will be cancelled
// to signal for them to being gracefully shutting down. If the routines all exit gracefully, or if
// the grace period is reached, the program will exit using os.Exit.
//
// The first error encountered is returned, except for context-related errors.
//
// Routines are started in the order they're given. If they utilize the given `initialized` channel
// then they will also wait for the previous routine to signal that it has started before moving on
// to the next routine. Handy for doing things like warming up caches or populating in-memory
// stores.
func RunE(gracePeriod time.Duration, routines ...Routine) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 2)
	signal.Notify(sigCh, os.Interrupt)
	defer signal.Stop(sigCh)

	go func() {
		<-sigCh
		fmt.Println("caught interrupt, cancelling")
		cancel()

		select {
		case <-time.After(gracePeriod):
		case <-sigCh:
		}

		os.Exit(1)
	}()

	g, ctx := errgroup.WithContext(ctx)

	var initErr error
	for _, routine := range routines {
		initialized := make(chan error)
		g.Go(func() error {
			return routine.Run(ctx, initialized)
		})
		// Wait for this routine to start before moving on. Capture failures.
		if initErr = <-initialized; initErr != nil {
			cancel()
		}
	}

	err := g.Wait()

	// For the following error checks, we don't propagate context.Canceled because it's expected to
	// be cancelled. It might've been cancelled by the previous routing failing to initialize too.

	// If we got an initialization error, we'll report that over anything else, as it's probably the
	// most relevant one to see.
	if initErr != nil && !errors.Is(err, context.Canceled) {
		return initErr
	}

	// Otherwise, we check if there was an error in-flight with any of the routines.
	if err != nil && !errors.Is(err, context.Canceled) {
		return err
	}

	return nil
}
