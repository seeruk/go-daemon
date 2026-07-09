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
