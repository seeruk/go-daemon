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

// Routine represents a background routine to be run. The given context is used to signal
// cancellation and should be used by the Routine implementation to stop gracefully.
type Routine interface {
	// Run attempts to start this Routine. This function should block until the Routine has ended.
	// The returned error should be nil if the Routine ended successfully, or an error if it ended
	// with an error.
	//
	// The started channel may be used to signal the Routine has finished preparing to start, and is
	// moving into the "started" state. If this isn't necessary, the channel can simply be closed
	// immediately, but this channel MUST be closed or the further routines will not be started.
	Run(ctx context.Context, started chan<- error) error
}

// RoutineFunc wraps a function that matches the signature of Routine.Run into a Routine.
// Useful for adapting compatible functions into a Routine.
type RoutineFunc func(ctx context.Context, started chan<- error) error

// Run this RoutineFunc.
func (fn RoutineFunc) Run(ctx context.Context, started chan<- error) error {
	return fn(ctx, started)
}

// SimpleRoutineFunc wraps a function that has a simpler signature than Routine.Run into a Routine.
// Useful for simple routines, and adapting compatible functions into a Routine.
type SimpleRoutineFunc func(ctx context.Context) error

// Run this SimpleRoutineFunc.
func (fn SimpleRoutineFunc) Run(ctx context.Context, started chan<- error) error {
	close(started) // Unused in this case, but we this means we can't guarantee ordering.
	return fn(ctx)
}

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
// If they're terminated, the process will wait for the given grace period.
// The first error encountered is returned, except for context-related errors.
//
// Routines are started in the order they're given. If they utilize the given `started` channel then
// they will also wait for the previous routine to signal that it has started before moving on to
// the next routine. Handy for doing things like warming up caches or populating in-memory stores.
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

	for _, routine := range routines {
		started := make(chan error)
		g.Go(func() error {
			return routine.Run(ctx, started)
		})
		<-started // Wait for this routine to start before moving on.
	}

	// We don't propagate context.Canceled because it's expected to be cancelled!
	if err := g.Wait(); err != nil && errors.Is(err, context.Canceled) {
		return err
	}

	return nil
}
