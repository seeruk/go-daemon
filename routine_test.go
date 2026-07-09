package daemon

import (
	"context"
	"errors"
	"testing"

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
