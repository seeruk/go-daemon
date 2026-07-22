package daemon

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRoutineFunc(t *testing.T) {
	called := false

	err := RoutineFunc(func(ctx context.Context) error {
		called = true
		return nil
	}).Run(context.Background())

	require.NoError(t, err)
	assert.True(t, called)
}

func TestPeriodicRoutine(t *testing.T) {
	t.Run("runs the function periodically and blocks until cancellation", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			calls := make(chan context.Context, 2)
			errCh := make(chan error, 1)

			routine := PeriodicRoutine(time.Second, func(ctx context.Context) error {
				calls <- ctx
				return nil
			})

			go func() {
				errCh <- routine.Run(ctx)
			}()

			time.Sleep(time.Second - time.Nanosecond)
			synctest.Wait()
			assert.Empty(t, calls)
			assert.Empty(t, errCh)

			time.Sleep(time.Nanosecond)
			synctest.Wait()
			assert.Same(t, ctx, <-calls)
			assert.Empty(t, errCh)

			time.Sleep(time.Second)
			synctest.Wait()
			assert.Same(t, ctx, <-calls)
			assert.Empty(t, errCh)

			cancel()
			synctest.Wait()
			require.ErrorIs(t, <-errCh, context.Canceled)
		})
	})

	t.Run("returns an error from the function", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			runErr := errors.New("run")
			errCh := make(chan error, 1)
			calls := 0

			routine := PeriodicRoutine(time.Second, func(ctx context.Context) error {
				calls++
				return runErr
			})

			go func() {
				errCh <- routine.Run(context.Background())
			}()

			time.Sleep(time.Second)
			synctest.Wait()
			require.ErrorIs(t, <-errCh, runErr)
			assert.Equal(t, 1, calls)
		})
	})
}

func TestInitializableRoutineAdapter(t *testing.T) {
	initErr := errors.New("init")
	runErr := errors.New("run")

	adapter := InitializableRoutineAdapter{
		InitFunc: func(ctx context.Context) error { return initErr },
		RunFunc:  func(ctx context.Context) error { return runErr },
	}

	require.ErrorIs(t, adapter.Initialize(context.Background()), initErr)
	require.ErrorIs(t, adapter.Run(context.Background()), runErr)
}
