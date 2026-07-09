package daemon

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_run(t *testing.T) {
	t.Run("should panic if given no routines", func(t *testing.T) {
		sigCh := make(chan os.Signal, 1)
		assert.Panics(t, func() {
			_ = run(sigCh, time.Second)
		})
	})

	t.Run("should block until routines are cancelled", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			sigCh := make(chan os.Signal, 1)
			errCh := make(chan error, 1)

			go func() {
				errCh <- run(sigCh, time.Second, basicRoutine(), basicRoutine())
			}()

			synctest.Wait()
			sigCh <- os.Interrupt

			synctest.Wait()
			assert.NoError(t, <-errCh)
		})
	})

	t.Run("should immediately return ErrInterrupted if signalled twice", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			sigCh := make(chan os.Signal, 1)
			errCh := make(chan error, 1)
			release := make(chan struct{})

			go func() {
				errCh <- run(sigCh, time.Second, stuckRoutine(release))
			}()

			synctest.Wait()
			sigCh <- os.Interrupt

			synctest.Wait()
			sigCh <- os.Interrupt

			synctest.Wait()
			assert.ErrorIs(t, <-errCh, ErrInterrupted)

			close(release)
			synctest.Wait()
		})
	})

	t.Run("should start routines in the expected order", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			sigCh := make(chan os.Signal, 1)
			errCh := make(chan error, 1)
			eventsCh := make(chan string, 4)

			makeRoutine := func(n int) InitializableRoutineAdapter {
				return InitializableRoutineAdapter{
					InitFunc: func(ctx context.Context) error {
						eventsCh <- fmt.Sprintf("init:%d", n)
						return nil
					},
					RunFunc: func(ctx context.Context) error {
						eventsCh <- fmt.Sprintf("run:%d", n)
						<-ctx.Done()
						return ctx.Err()
					},
				}
			}

			go func() {
				errCh <- run(sigCh, time.Second, makeRoutine(1), makeRoutine(2))
			}()

			synctest.Wait()

			events := collectNFromChan(eventsCh, 4)
			require.Contains(t, events, "init:1")
			require.Contains(t, events, "run:1")
			require.Contains(t, events, "init:2")
			require.Contains(t, events, "run:2")

			// Here are the things we can "guarantee". Since daemon starts each routine in a
			// goroutine, it is possible for "init:2" to be seen before "run:1"
			assert.Less(t, slices.Index(events, "init:1"), slices.Index(events, "init:2"))
			assert.Less(t, slices.Index(events, "init:1"), slices.Index(events, "run:1"))
			assert.Less(t, slices.Index(events, "init:2"), slices.Index(events, "run:2"))

			sigCh <- os.Interrupt
			synctest.Wait()

			require.NoError(t, <-errCh)
		})
	})

	t.Run("should cancel started routines if a subsequent initialize fails", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			sigCh := make(chan os.Signal, 1)
			errCh := make(chan error, 1)

			started := make(chan string, 2)
			stopped := make(chan string, 2)

			initErr := errors.New("init error")

			makeRoutine := func(n int) InitializableRoutineAdapter {
				return InitializableRoutineAdapter{
					InitFunc: func(ctx context.Context) error {
						if n == 2 {
							return initErr
						}
						return nil
					},
					RunFunc: func(ctx context.Context) error {
						started <- fmt.Sprintf("run:%d", n)
						defer func() { stopped <- fmt.Sprintf("run:%d", n) }()
						<-ctx.Done()
						return ctx.Err()
					},
				}
			}

			go func() {
				errCh <- run(sigCh, time.Second, makeRoutine(1), makeRoutine(2))
			}()

			synctest.Wait()

			require.ErrorIs(t, <-errCh, initErr)
			assert.Equal(t, "run:1", <-started)
			assert.Equal(t, "run:1", <-stopped)

			select {
			case <-started:
				assert.Fail(t, "should not have started routine")
			case <-stopped:
				assert.Fail(t, "should not have stopped routine")
			default:
				// We should hit this and then be done
			}
		})
	})

	t.Run("should not cancel other routines if a routine returns ErrCompleted", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			sigCh := make(chan os.Signal, 1)
			errCh := make(chan error, 1)

			completed := make(chan string, 1)
			started := make(chan string, 1)
			stopped := make(chan string, 1)

			go func() {
				errCh <- run(sigCh, time.Second,
					RoutineFunc(func(ctx context.Context) error {
						completed <- "run:1"
						return ErrCompleted
					}),
					RoutineFunc(func(ctx context.Context) error {
						started <- "run:2"
						defer func() { stopped <- "run:2" }()
						<-ctx.Done()
						return ctx.Err()
					}),
				)
			}()

			synctest.Wait()

			assert.Equal(t, "run:1", <-completed)
			assert.Equal(t, "run:2", <-started)

			select {
			case <-stopped:
				assert.Fail(t, "should not have stopped routine")
			default:
				// We should hit this and then be done
			}

			sigCh <- os.Interrupt
			synctest.Wait()

			require.NoError(t, <-errCh)
			assert.Equal(t, "run:2", <-stopped)
		})
	})

	t.Run("should return nil if all routines return ErrCompleted", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			sigCh := make(chan os.Signal, 1)
			errCh := make(chan error, 1)

			go func() {
				errCh <- run(sigCh, time.Second,
					RoutineFunc(func(ctx context.Context) error {
						return ErrCompleted
					}),
					RoutineFunc(func(ctx context.Context) error {
						return ErrCompleted
					}),
				)
			}()

			synctest.Wait()
			assert.NoError(t, <-errCh)
		})
	})

	t.Run("should cancel other routines if a routine errors", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			sigCh := make(chan os.Signal, 1)
			errCh := make(chan error, 1)

			started := make(chan struct{})
			stopped := make(chan string, 1)
			runErr := errors.New("run error")

			go func() {
				errCh <- run(sigCh, time.Second,
					RoutineFunc(func(ctx context.Context) error {
						close(started)
						defer func() { stopped <- "run:1" }()
						<-ctx.Done()
						return ctx.Err()
					}),
					RoutineFunc(func(ctx context.Context) error {
						<-started
						return runErr
					}),
				)
			}()

			synctest.Wait()

			require.ErrorIs(t, <-errCh, runErr)
			assert.Equal(t, "run:1", <-stopped)
		})
	})

	t.Run("should return grace period exceeded if routines do not stop", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			sigCh := make(chan os.Signal, 1)
			errCh := make(chan error, 1)
			release := make(chan struct{})

			go func() {
				errCh <- run(sigCh, time.Second, stuckRoutine(release))
			}()

			synctest.Wait()
			sigCh <- os.Interrupt
			synctest.Wait()

			require.ErrorIs(t, <-errCh, ErrGracePeriodExceeded)

			close(release)
			synctest.Wait()
		})
	})

	t.Run("should preserve stop error if routines do not stop", func(t *testing.T) {
		t.Run("from initialize", func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				sigCh := make(chan os.Signal, 1)
				errCh := make(chan error, 1)
				release := make(chan struct{})
				initErr := errors.New("init error")

				go func() {
					errCh <- run(sigCh, time.Second,
						stuckRoutine(release),
						InitializableRoutineAdapter{
							InitFunc: func(ctx context.Context) error {
								return initErr
							},
							RunFunc: func(ctx context.Context) error {
								return nil
							},
						},
					)
				}()

				synctest.Wait()

				err := <-errCh
				require.ErrorIs(t, err, ErrGracePeriodExceeded)
				require.ErrorIs(t, err, initErr)

				close(release)
				synctest.Wait()
			})
		})

		t.Run("from run", func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				sigCh := make(chan os.Signal, 1)
				errCh := make(chan error, 1)
				release := make(chan struct{})
				runErr := errors.New("run error")

				go func() {
					errCh <- run(sigCh, time.Second,
						stuckRoutine(release),
						RoutineFunc(func(ctx context.Context) error {
							return runErr
						}),
					)
				}()

				synctest.Wait()

				err := <-errCh
				require.ErrorIs(t, err, ErrGracePeriodExceeded)
				require.ErrorIs(t, err, runErr)

				close(release)
				synctest.Wait()
			})
		})
	})
}

func basicRoutine() RoutineFunc {
	return blockingRunFunc
}

func blockingRunFunc(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

func stuckRoutine(release <-chan struct{}) RoutineFunc {
	return func(ctx context.Context) error {
		<-release
		return nil
	}
}

func collectNFromChan[T any](ch chan T, n int) []T {
	var out []T
	for range n {
		out = append(out, <-ch)
	}
	return out
}
