package rungroup_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/uw-labs/proximo/pkg/rungroup"
)

func run(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}

func TestGroup_DontWaitForAsync(t *testing.T) {
	assert := require.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()

	wg := sync.WaitGroup{}
	wg.Add(1)
	g, ctx := rungroup.New(ctx)
	var asyncFinished time.Time

	g.GoAsync(func() error {
		defer wg.Done()
		time.Sleep(time.Millisecond * 500)
		asyncFinished = time.Now()
		return nil
	})
	g.Go(func() error {
		return run(ctx, time.Millisecond*200)
	})
	g.Go(func() error {
		return run(ctx, time.Millisecond*200)
	})

	assert.NoError(g.Wait())
	syncFinished := time.Now()
	wg.Wait()
	assert.True(asyncFinished.After(syncFinished))
}

func TestGroup_StopOnError(t *testing.T) {
	assert := require.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()

	var syncErr, asyncErr error
	expectedErr := errors.New("component stopped")
	g, ctx := rungroup.New(ctx)

	g.GoAsync(func() error {
		asyncErr = run(ctx, time.Second)
		return asyncErr
	})
	g.Go(func() error {
		syncErr = run(ctx, time.Second)
		return syncErr
	})
	g.Go(func() error {
		time.Sleep(time.Millisecond * 50)
		return expectedErr
	})

	assert.Equal(expectedErr, g.Wait())
	assert.Equal(context.Canceled, syncErr)
	assert.Equal(context.Canceled, asyncErr)
}

func TestGroup_StopOnTermination(t *testing.T) {
	assert := require.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()

	var sync1Err, sync2Err, asyncErr error
	g, ctx := rungroup.New(ctx)

	g.GoAsync(func() error {
		asyncErr = run(ctx, time.Second)
		return asyncErr
	})
	g.Go(func() error {
		sync2Err = run(ctx, time.Second)
		return sync2Err
	})
	g.Go(func() error {
		sync1Err = run(ctx, time.Millisecond*50)
		return sync1Err
	})

	assert.Equal(context.Canceled, g.Wait())
	assert.Equal(nil, sync1Err)
	assert.Equal(context.Canceled, sync2Err)
	assert.Equal(context.Canceled, asyncErr)
}
