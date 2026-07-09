package daemon

import "context"

// Routine represents a background routine to be run. The given context is used to signal
// cancellation and should be used by the Routine implementation to stop gracefully.
type Routine interface {
	// Run attempts to start this Routine. This function should block until the Routine has ended.
	// The returned error should be nil if the Routine ended successfully, or an error if it ended
	// with an error.
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
