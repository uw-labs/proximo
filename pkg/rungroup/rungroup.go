// Copyright 2016 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// This is an adaptation of golang.org/x/sync/errgroup
// to satisfy needs of this project.
package rungroup

import (
	"context"
	"sync"
	"sync/atomic"
)

// Group is a collection of goroutines serving one client request. The underlying context
// is cancelled as soon as any goroutine terminates regardless of the outcome.
type Group interface {
	// Wait blocks until all function calls from the Go method have returned, then
	// returns the first non-nil error (if any) from them. Note that it won't wait
	// for functions started with GoAsync.
	Wait() error
	// Go calls the given function in a new goroutine.
	Go(f func() error)
	// GoAsync behaves the same way Go does, except the call to Wait won't wait for this function to finish.
	GoAsync(f func() error)
}

type group struct {
	cancel func()

	wg sync.WaitGroup

	errOnce sync.Once
	err     atomic.Value
}

// New returns a new Group and an associated Context derived from the provided one.
// The context is cancelled when the first goroutine started with Go or GoAsync returns
// regardless of the outcome.
func New(ctx context.Context) (*group, context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	return &group{cancel: cancel}, ctx
}

func (g *group) Wait() error {
	g.wg.Wait()
	if g.cancel != nil {
		g.cancel()
	}

	if err := g.err.Load(); err != nil {
		return err.(error)
	}
	return nil
}

func (g *group) runFunc(f func() error) {
	if err := f(); err != nil {
		g.errOnce.Do(func() {
			g.err.Store(err)
		})
	}
	if g.cancel != nil {
		g.cancel()
	}
}

func (g *group) Go(f func() error) {
	g.wg.Add(1)

	go func() {
		defer g.wg.Done()
		g.runFunc(f)
	}()
}

func (g *group) GoAsync(f func() error) {
	go g.runFunc(f)
}
