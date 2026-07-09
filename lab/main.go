package main

import (
	"context"
	"os"
	"time"

	"github.com/seeruk/go-daemon"
)

func main() {
	os.Exit(daemon.Run(time.Second,
		daemon.RoutineFunc(func(ctx context.Context) error {
			select {
			case <-time.After(60 * time.Second):
			case <-ctx.Done():
				return ctx.Err()
			}
			return nil
		}),
		daemon.RoutineFunc(func(ctx context.Context) error {
			select {
			case <-time.After(60 * time.Second):
			case <-ctx.Done():
				return ctx.Err()
			}
			return nil
		}),
	))
}
