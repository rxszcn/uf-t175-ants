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

// LoadBalancingStrategy represents the type of load-balancing algorithm.
type LoadBalancingStrategy int

const (
	// RoundRobin distributes task to a list of pools in rotation.
	RoundRobin LoadBalancingStrategy = 1 << (iota + 1)

	// LeastTasks always selects the pool with the least number of pending tasks.
	LeastTasks
)

// MultiPool consists of multiple pools, from which you will benefit the
// performance improvement on basis of the fine-grained locking that reduces
// the lock contention.
// MultiPool is a good fit for the scenario where you have a large number of
// tasks to submit, and you don't want the single pool to be the bottleneck.
type MultiPool struct {
	multiPoolCore[*Pool]
}

// NewMultiPool instantiates a MultiPool with a size of the pool list and a size
// per pool, and the load-balancing strategy.
func NewMultiPool(size, sizePerPool int, lbs LoadBalancingStrategy, options ...Option) (*MultiPool, error) {
	core, err := newMultiPoolCore(size, sizePerPool, lbs, NewPool, options...)
	if err != nil {
		return nil, err
	}
	return &MultiPool{multiPoolCore: *core}, nil
}

// Submit submits a task to a pool selected by the load-balancing strategy.
func (mp *MultiPool) Submit(task func()) (err error) {
	if mp.IsClosed() {
		return ErrPoolClosed
	}
	if err = mp.pools[mp.next(mp.lbs)].Submit(task); err == nil {
		return
	}
	if err == ErrPoolOverload && mp.lbs == RoundRobin {
		return mp.pools[mp.next(LeastTasks)].Submit(task)
	}
	return
}
