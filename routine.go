package daemon

import (
	"context"
	"time"
)

// Routine represents a background routine to be run. The given context is used to signal
// cancellation and should be used by the Routine implementation to stop gracefully.
type Routine interface {
	// Run attempts to start this Routine. This function should block until the Routine has ended.
	// The returned error should be nil if the Routine ended successfully, an error if it ended
	// with an error, or the sentinel ErrCompleted if the routine is intentionally finished and
	// shouldn't stop other routines.
	Run(ctx context.Context) error
}

// InitializableRoutine is a specialized Routine that has an initialization step. Handy for when
// your Routine needs to do some work before starting, like populating in-memory stores, warming up
// caches, etc.
type InitializableRoutine interface {
	Routine
	// Initialize attempts to initialize this Routine.
	Initialize(ctx context.Context) error
}

// RoutineFunc is a convenience type for creating Routines from functions.
// It's useful for quickly creating a routine as a wrapper around a Routine-shaped function.
type RoutineFunc func(ctx context.Context) error

// Run starts this RoutineFunc.
func (fn RoutineFunc) Run(ctx context.Context) error {
	return fn(ctx)
}

// PeriodicRoutine creates a RoutineFunc that runs runFunc at a fixed interval. Calls are made
// sequentially; a call that takes longer than interval will not overlap with another call.
//
// The returned RoutineFunc blocks until its context is cancelled or runFunc returns an error.
// PeriodicRoutine expects interval to be greater than zero and runFunc to be set.
func PeriodicRoutine(interval time.Duration, runFunc RoutineFunc) RoutineFunc {
	return func(ctx context.Context) error {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				if err := runFunc(ctx); err != nil {
					return err
				}
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}

// InitializableRoutineAdapter is a convenience type for creating InitializableRoutines
// from a pair of functions.
//
// InitializableRoutineAdapter expects both InitFunc and RunFunc to be set.
type InitializableRoutineAdapter struct {
	InitFunc func(ctx context.Context) error
	RunFunc  func(ctx context.Context) error
}

// Initialize attempts to initialize this InitializableRoutineAdapter.
func (adapter InitializableRoutineAdapter) Initialize(ctx context.Context) error {
	return adapter.InitFunc(ctx)
}

// Run attempts to start this InitializableRoutineAdapter.
func (adapter InitializableRoutineAdapter) Run(ctx context.Context) error {
	return adapter.RunFunc(ctx)
}
