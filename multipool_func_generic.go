// MIT License

// Copyright (c) 2025 Andy Pan

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

// MultiPoolWithFuncGeneric is the generic version of MultiPoolWithFunc.
type MultiPoolWithFuncGeneric[T any] struct {
	multiPoolCore[*PoolWithFuncGeneric[T]]
}

// NewMultiPoolWithFuncGeneric instantiates a MultiPoolWithFunc with a size of the pool list and a size
// per pool, and the load-balancing strategy.
func NewMultiPoolWithFuncGeneric[T any](size, sizePerPool int, fn func(T), lbs LoadBalancingStrategy, options ...Option) (*MultiPoolWithFuncGeneric[T], error) {
	core, err := newMultiPoolCore(size, sizePerPool, lbs, func(poolSize int, options ...Option) (*PoolWithFuncGeneric[T], error) {
		return NewPoolWithFuncGeneric(poolSize, fn, options...)
	}, options...)
	if err != nil {
		return nil, err
	}
	return &MultiPoolWithFuncGeneric[T]{multiPoolCore: *core}, nil
}

// Invoke submits a task to a pool selected by the load-balancing strategy.
func (mp *MultiPoolWithFuncGeneric[T]) Invoke(args T) (err error) {
	if mp.IsClosed() {
		return ErrPoolClosed
	}

	if err = mp.pools[mp.next(mp.lbs)].Invoke(args); err == nil {
		return
	}
	if err == ErrPoolOverload && mp.lbs == RoundRobin {
		return mp.pools[mp.next(LeastTasks)].Invoke(args)
	}
	return
}
