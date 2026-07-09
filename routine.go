package daemon

import "context"

// Routine represents a background routine to be run. The given context is used to signal
// cancellation and should be used by the Routine implementation to stop gracefully.
type Routine interface {
	// Run attempts to start this Routine. This function should block until the Routine has ended.
	// The returned error should be nil if the Routine ended successfully, or an error if it ended
	// with an error.
	//
	// The initialized channel may be used to signal the Routine has finished preparing to start,
	// and is moving onto starting. Not all routines will need to take advantage of this, but this
	// channel MUST be closed by the routine, or further threads will not be run. This is useful for
	// threads which need to prepare something before starting, like an in-memory store, or doing
	// some kind of cache-warming.
	Run(ctx context.Context, initialized chan<- error) error
}

// RoutineFunc wraps a function that matches the signature of Routine.Run into a Routine.
// Useful for adapting compatible functions into a Routine.
type RoutineFunc func(ctx context.Context, initialized chan<- error) error

// Run this RoutineFunc.
func (fn RoutineFunc) Run(ctx context.Context, initialized chan<- error) error {
	return fn(ctx, initialized)
}

// SimpleRoutineFunc wraps a function that has a simpler signature than Routine.Run into a Routine.
// Useful for simple routines, and adapting compatible functions into a Routine.
type SimpleRoutineFunc func(ctx context.Context) error

// Run this SimpleRoutineFunc.
func (fn SimpleRoutineFunc) Run(ctx context.Context, initialized chan<- error) error {
	close(initialized) // Unused in this case, but we this means we can't guarantee ordering.
	return fn(ctx)
}
