// MIT License

// Copyright (c) 2023 Andy Pan

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package ants

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"
)

type contextReleaser interface {
	ReleaseContext(ctx context.Context) error
}

func releasePools(ctx context.Context, pools []contextReleaser) error {
	errCh := make(chan error, len(pools))
	var wg errgroup.Group
	for i, pool := range pools {
		func(p contextReleaser, idx int) {
			wg.Go(func() error {
				err := p.ReleaseContext(ctx)
				if err != nil {
					err = fmt.Errorf("pool %d: %v", idx, err)
				}
				errCh <- err
				return err
			})
		}(pool, i)
	}

	_ = wg.Wait()

	var errStr strings.Builder
	for i := 0; i < len(pools); i++ {
		if err := <-errCh; err != nil {
			errStr.WriteString(err.Error())
			errStr.WriteString(" | ")
		}
	}

	if errStr.Len() == 0 {
		return nil
	}

	return errors.New(strings.TrimSuffix(errStr.String(), " | "))
}

// multiPoolMember is the set of behaviors that every pool held by a multi-pool
// must provide. It is implemented by Pool, PoolWithFunc and
// PoolWithFuncGeneric[T], which lets multiPoolCore serve all of them.
type multiPoolMember interface {
	contextReleaser

	Running() int
	Free() int
	Waiting() int
	Cap() int
	Tune(size int)
	Release()
	Reboot()
}

// multiPoolCore holds the pool set and the load-balancing state shared by
// MultiPool, MultiPoolWithFunc and MultiPoolWithFuncGeneric.
//
// The three public multi-pool types embed an instantiation of this core, so the
// scheduling, bookkeeping and lifecycle logic has a single implementation
// instead of drifting apart across the entry points.
type multiPoolCore[P multiPoolMember] struct {
	pools []P
	index uint32
	state int32
	lbs   LoadBalancingStrategy
}

func newMultiPoolCore[P multiPoolMember](
	size, sizePerPool int,
	lbs LoadBalancingStrategy,
	newPool func(poolSize int, options ...Option) (P, error),
	options ...Option,
) (*multiPoolCore[P], error) {
	if size <= 0 {
		return nil, ErrInvalidMultiPoolSize
	}

	if lbs != RoundRobin && lbs != LeastTasks {
		return nil, ErrInvalidLoadBalancingStrategy
	}
	pools := make([]P, size)
	for i := 0; i < size; i++ {
		pool, err := newPool(sizePerPool, options...)
		if err != nil {
			// Release all previously created pools to avoid resource leak
			for j := 0; j < i; j++ {
				pools[j].Release()
			}
			return nil, err
		}
		pools[i] = pool
	}
	return &multiPoolCore[P]{pools: pools, index: math.MaxUint32, lbs: lbs}, nil
}

func (mp *multiPoolCore[P]) next(lbs LoadBalancingStrategy) (idx int) {
	switch lbs {
	case RoundRobin:
		return int(atomic.AddUint32(&mp.index, 1) % uint32(len(mp.pools)))
	case LeastTasks:
		leastTasks := math.MaxInt32
		for i, pool := range mp.pools {
			if n := pool.Running(); n < leastTasks {
				leastTasks = n
				idx = i
			}
		}
		return
	}
	return -1
}

// Running returns the number of the currently running workers across all pools.
func (mp *multiPoolCore[P]) Running() (n int) {
	for _, pool := range mp.pools {
		n += pool.Running()
	}
	return
}

// RunningByIndex returns the number of the currently running workers in the specific pool.
func (mp *multiPoolCore[P]) RunningByIndex(idx int) (int, error) {
	if idx < 0 || idx >= len(mp.pools) {
		return -1, ErrInvalidPoolIndex
	}
	return mp.pools[idx].Running(), nil
}

// Free returns the number of available workers across all pools.
func (mp *multiPoolCore[P]) Free() (n int) {
	for _, pool := range mp.pools {
		n += pool.Free()
	}
	return
}

// FreeByIndex returns the number of available workers in the specific pool.
func (mp *multiPoolCore[P]) FreeByIndex(idx int) (int, error) {
	if idx < 0 || idx >= len(mp.pools) {
		return -1, ErrInvalidPoolIndex
	}
	return mp.pools[idx].Free(), nil
}

// Waiting returns the number of the currently waiting tasks across all pools.
func (mp *multiPoolCore[P]) Waiting() (n int) {
	for _, pool := range mp.pools {
		n += pool.Waiting()
	}
	return
}

// WaitingByIndex returns the number of the currently waiting tasks in the specific pool.
func (mp *multiPoolCore[P]) WaitingByIndex(idx int) (int, error) {
	if idx < 0 || idx >= len(mp.pools) {
		return -1, ErrInvalidPoolIndex
	}
	return mp.pools[idx].Waiting(), nil
}

// Cap returns the capacity of this multi-pool.
func (mp *multiPoolCore[P]) Cap() (n int) {
	for _, pool := range mp.pools {
		n += pool.Cap()
	}
	return
}

// Tune resizes each pool in multi-pool.
//
// Note that this method doesn't resize the overall
// capacity of multi-pool.
func (mp *multiPoolCore[P]) Tune(size int) {
	for _, pool := range mp.pools {
		pool.Tune(size)
	}
}

// IsClosed indicates whether the multi-pool is closed.
func (mp *multiPoolCore[P]) IsClosed() bool {
	return atomic.LoadInt32(&mp.state) == CLOSED
}

// ReleaseTimeout closes the multi-pool with a timeout,
// it waits all pools to be closed before timing out.
func (mp *multiPoolCore[P]) ReleaseTimeout(timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return mp.ReleaseContext(ctx)
}

// ReleaseContext closes the multi-pool with a context,
// it waits all pools to be closed before the context is done.
func (mp *multiPoolCore[P]) ReleaseContext(ctx context.Context) error {
	if !atomic.CompareAndSwapInt32(&mp.state, OPENED, CLOSED) {
		return ErrPoolClosed
	}

	pools := make([]contextReleaser, len(mp.pools))
	for i, p := range mp.pools {
		pools[i] = p
	}
	return releasePools(ctx, pools)
}

// Reboot reboots a released multi-pool.
func (mp *multiPoolCore[P]) Reboot() {
	if atomic.CompareAndSwapInt32(&mp.state, CLOSED, OPENED) {
		atomic.StoreUint32(&mp.index, 0)
		for _, pool := range mp.pools {
			pool.Reboot()
		}
	}
}
