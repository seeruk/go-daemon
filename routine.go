package daemon

import "context"

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
