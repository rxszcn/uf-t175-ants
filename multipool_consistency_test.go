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

package ants_test

import (
	"context"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/panjf2000/ants/v2"
)

// multiPoolAPI is the set of behaviors shared by the three multi-pool entry
// points. Declaring it here turns a signature divergence on any of them into a
// compile error in this test file rather than a silent behavioral drift.
type multiPoolAPI interface {
	submit(blocker chan struct{}) error

	Running() int
	RunningByIndex(idx int) (int, error)
	Free() int
	FreeByIndex(idx int) (int, error)
	Waiting() int
	WaitingByIndex(idx int) (int, error)
	Cap() int
	Tune(size int)
	IsClosed() bool
	ReleaseTimeout(timeout time.Duration) error
	ReleaseContext(ctx context.Context) error
	Reboot()
}

// multiPoolSubmit adapts MultiPool.Submit to the shared submit shape.
type multiPoolSubmit struct {
	*ants.MultiPool
}

func (mp multiPoolSubmit) submit(blocker chan struct{}) error {
	return mp.Submit(func() { <-blocker })
}

// multiPoolInvoke adapts MultiPoolWithFunc.Invoke to the shared submit shape.
type multiPoolInvoke struct {
	*ants.MultiPoolWithFunc
}

func (mp multiPoolInvoke) submit(blocker chan struct{}) error {
	return mp.Invoke(blocker)
}

// multiPoolInvokeGeneric adapts MultiPoolWithFuncGeneric.Invoke to the shared submit shape.
type multiPoolInvokeGeneric struct {
	*ants.MultiPoolWithFuncGeneric[chan struct{}]
}

func (mp multiPoolInvokeGeneric) submit(blocker chan struct{}) error {
	return mp.Invoke(blocker)
}

var (
	_ multiPoolAPI = multiPoolSubmit{}
	_ multiPoolAPI = multiPoolInvoke{}
	_ multiPoolAPI = multiPoolInvokeGeneric{}
)

// multiPoolEntryPoint describes one of the three multi-pool constructors.
type multiPoolEntryPoint struct {
	name string
	open func(size, sizePerPool int, lbs ants.LoadBalancingStrategy, options ...ants.Option) (multiPoolAPI, error)
}

var multiPoolEntryPoints = []multiPoolEntryPoint{
	{
		name: "MultiPool",
		open: func(size, sizePerPool int, lbs ants.LoadBalancingStrategy, options ...ants.Option) (multiPoolAPI, error) {
			mp, err := ants.NewMultiPool(size, sizePerPool, lbs, options...)
			if err != nil {
				return nil, err
			}
			return multiPoolSubmit{mp}, nil
		},
	},
	{
		name: "MultiPoolWithFunc",
		open: func(size, sizePerPool int, lbs ants.LoadBalancingStrategy, options ...ants.Option) (multiPoolAPI, error) {
			mp, err := ants.NewMultiPoolWithFunc(size, sizePerPool, func(arg any) { <-arg.(chan struct{}) }, lbs, options...)
			if err != nil {
				return nil, err
			}
			return multiPoolInvoke{mp}, nil
		},
	},
	{
		name: "MultiPoolWithFuncGeneric",
		open: func(size, sizePerPool int, lbs ants.LoadBalancingStrategy, options ...ants.Option) (multiPoolAPI, error) {
			mp, err := ants.NewMultiPoolWithFuncGeneric(size, sizePerPool, func(blocker chan struct{}) { <-blocker }, lbs, options...)
			if err != nil {
				return nil, err
			}
			return multiPoolInvokeGeneric{mp}, nil
		},
	},
}

// multiPoolIndexStat is the observable state of a single pool in the group.
type multiPoolIndexStat struct {
	Running int
	Free    int
	Waiting int
}

// multiPoolStat is the observable state of a multi-pool at one point in time.
type multiPoolStat struct {
	Running int
	Free    int
	Waiting int
	Cap     int
	Closed  bool
	PerPool []multiPoolIndexStat
}

// multiPoolScenarioResult captures every observable state a multi-pool goes
// through in the shared scenario below.
type multiPoolScenarioResult struct {
	Fresh          multiPoolStat
	Loaded         multiPoolStat
	Tuned          multiPoolStat
	Drained        multiPoolStat
	Closed         multiPoolStat
	ClosedSubmit   string
	ClosedRelease  string
	Rebooted       multiPoolStat
	RebootedSubmit string
}

func multiPoolErrString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// multiPoolReleaseErrFragments normalizes a release error into its sorted
// per-pool fragments. releasePools() collects errors in completion order, so
// the concatenated string is inherently order-unstable; only the set of
// per-pool failures is a contract worth asserting on.
func multiPoolReleaseErrFragments(err error) []string {
	if err == nil {
		return nil
	}
	fragments := strings.Split(err.Error(), " | ")
	sort.Strings(fragments)
	return fragments
}

// snapshotMultiPool reads every observable field and also probes the
// out-of-range index contract.
func snapshotMultiPool(t *testing.T, mp multiPoolAPI, size int) multiPoolStat {
	t.Helper()

	stat := multiPoolStat{
		Running: mp.Running(),
		Free:    mp.Free(),
		Waiting: mp.Waiting(),
		Cap:     mp.Cap(),
		Closed:  mp.IsClosed(),
	}
	for i := 0; i < size; i++ {
		running, err := mp.RunningByIndex(i)
		require.NoError(t, err)
		free, err := mp.FreeByIndex(i)
		require.NoError(t, err)
		waiting, err := mp.WaitingByIndex(i)
		require.NoError(t, err)
		stat.PerPool = append(stat.PerPool, multiPoolIndexStat{Running: running, Free: free, Waiting: waiting})
	}

	for _, idx := range []int{-1, size} {
		_, err := mp.RunningByIndex(idx)
		require.ErrorIs(t, err, ants.ErrInvalidPoolIndex)
		_, err = mp.FreeByIndex(idx)
		require.ErrorIs(t, err, ants.ErrInvalidPoolIndex)
		_, err = mp.WaitingByIndex(idx)
		require.ErrorIs(t, err, ants.ErrInvalidPoolIndex)
	}

	return stat
}

// runMultiPoolScenario drives a multi-pool through the states that all three
// entry points must agree on: construction, saturation, tune, drain, release
// and reboot.
func runMultiPoolScenario(t *testing.T, ep multiPoolEntryPoint, size, sizePerPool int, lbs ants.LoadBalancingStrategy) multiPoolScenarioResult {
	t.Helper()

	mp, err := ep.open(size, sizePerPool, lbs)
	require.NoError(t, err)

	var result multiPoolScenarioResult
	result.Fresh = snapshotMultiPool(t, mp, size)

	blocker := make(chan struct{})
	for i := 0; i < size*sizePerPool; i++ {
		require.NoError(t, mp.submit(blocker))
	}
	require.Eventually(t, func() bool {
		return mp.Running() == size*sizePerPool
	}, 3*time.Second, 10*time.Millisecond)
	result.Loaded = snapshotMultiPool(t, mp, size)

	// Tune every pool in the group; the group capacity must grow accordingly.
	mp.Tune(sizePerPool + 2)
	result.Tuned = snapshotMultiPool(t, mp, size)

	close(blocker)
	require.Eventually(t, func() bool {
		return mp.Running() == 0
	}, 3*time.Second, 10*time.Millisecond)
	result.Drained = snapshotMultiPool(t, mp, size)

	require.NoError(t, mp.ReleaseTimeout(3*time.Second))
	result.Closed = snapshotMultiPool(t, mp, size)
	result.ClosedSubmit = multiPoolErrString(mp.submit(make(chan struct{})))
	result.ClosedRelease = multiPoolErrString(mp.ReleaseTimeout(3 * time.Second))

	mp.Reboot()
	result.Rebooted = snapshotMultiPool(t, mp, size)
	rebootedBlocker := make(chan struct{})
	result.RebootedSubmit = multiPoolErrString(mp.submit(rebootedBlocker))
	close(rebootedBlocker)

	require.NoError(t, mp.ReleaseTimeout(3*time.Second))

	return result
}

// TestMultiPoolEntryPointsConsistency asserts that MultiPool,
// MultiPoolWithFunc and MultiPoolWithFuncGeneric expose the same observable
// behavior for the same sequence of operations.
func TestMultiPoolEntryPointsConsistency(t *testing.T) {
	const (
		size        = 3
		sizePerPool = 5
	)

	strategies := []struct {
		name string
		lbs  ants.LoadBalancingStrategy
	}{
		{name: "RoundRobin", lbs: ants.RoundRobin},
		{name: "LeastTasks", lbs: ants.LeastTasks},
	}

	for _, strategy := range strategies {
		strategy := strategy
		t.Run(strategy.name, func(t *testing.T) {
			t.Parallel()

			base := runMultiPoolScenario(t, multiPoolEntryPoints[0], size, sizePerPool, strategy.lbs)
			for _, ep := range multiPoolEntryPoints[1:] {
				got := runMultiPoolScenario(t, ep, size, sizePerPool, strategy.lbs)
				require.Equal(t, base, got, "%s diverged from %s under %s", ep.name, multiPoolEntryPoints[0].name, strategy.name)
			}
		})
	}
}

// recordMultiPoolRouting submits tasks one by one and records, after each
// submission, which pool received the worker.
func recordMultiPoolRouting(t *testing.T, mp multiPoolAPI, size, submissions int) (routing [][]int, drain func()) {
	t.Helper()

	blocker := make(chan struct{})
	for i := 0; i < submissions; i++ {
		require.NoError(t, mp.submit(blocker))
		vec := make([]int, size)
		for idx := 0; idx < size; idx++ {
			running, err := mp.RunningByIndex(idx)
			require.NoError(t, err)
			vec[idx] = running
		}
		routing = append(routing, vec)
	}
	return routing, func() { close(blocker) }
}

// TestMultiPoolEntryPointsRoutingConsistency asserts that multi-pool routing
// picks the same pool on every submission, before and after a rolling reboot.
// The rotation offset reset in Reboot() is part of the documented behavior, so
// all three entry points must apply it identically.
func TestMultiPoolEntryPointsRoutingConsistency(t *testing.T) {
	const (
		size        = 3
		sizePerPool = 3
		submissions = size * sizePerPool
	)

	strategies := []struct {
		name string
		lbs  ants.LoadBalancingStrategy
	}{
		{name: "RoundRobin", lbs: ants.RoundRobin},
		{name: "LeastTasks", lbs: ants.LeastTasks},
	}

	for _, strategy := range strategies {
		strategy := strategy
		t.Run(strategy.name, func(t *testing.T) {
			t.Parallel()

			type routingResult struct {
				before [][]int
				after  [][]int
			}

			run := func(ep multiPoolEntryPoint) routingResult {
				mp, err := ep.open(size, sizePerPool, strategy.lbs)
				require.NoError(t, err)

				before, drain := recordMultiPoolRouting(t, mp, size, submissions)
				drain()
				require.Eventually(t, func() bool {
					return mp.Running() == 0
				}, 3*time.Second, 10*time.Millisecond)
				require.NoError(t, mp.ReleaseTimeout(3*time.Second))

				mp.Reboot()
				after, drainAfter := recordMultiPoolRouting(t, mp, size, submissions)
				drainAfter()
				require.Eventually(t, func() bool {
					return mp.Running() == 0
				}, 3*time.Second, 10*time.Millisecond)
				require.NoError(t, mp.ReleaseTimeout(3*time.Second))

				return routingResult{before: before, after: after}
			}

			base := run(multiPoolEntryPoints[0])
			// The routing must be a real rotation, not everything landing on one pool.
			require.Len(t, base.before, submissions)
			for _, ep := range multiPoolEntryPoints[1:] {
				got := run(ep)
				require.Equal(t, base.before, got.before, "%s routing diverged from %s under %s before reboot", ep.name, multiPoolEntryPoints[0].name, strategy.name)
				require.Equal(t, base.after, got.after, "%s routing diverged from %s under %s after reboot", ep.name, multiPoolEntryPoints[0].name, strategy.name)
			}
		})
	}
}

// pickedPools replays a routing trace and reports which pool index received
// each submission.
func pickedPools(trace [][]int, size int) []int {
	picked := make([]int, 0, len(trace))
	var prev []int
	for i, vec := range trace {
		if i == 0 {
			prev = make([]int, size)
		}
		for idx, running := range vec {
			if running > prev[idx] {
				picked = append(picked, idx)
				break
			}
		}
		prev = vec
	}
	return picked
}

// TestMultiPoolZeroValueConsistency pins the behavior of the zero value of
// each multi-pool type. The shared core is embedded by value (not by pointer),
// so a zero multi-pool reports OPENED like it did when the state fields were
// plain struct fields, instead of dereferencing a nil core pointer.
func TestMultiPoolZeroValueConsistency(t *testing.T) {
	var (
		mp        ants.MultiPool
		mpFunc    ants.MultiPoolWithFunc
		mpGeneric ants.MultiPoolWithFuncGeneric[chan struct{}]
	)
	for _, zero := range []interface{ IsClosed() bool }{&mp, &mpFunc, &mpGeneric} {
		require.False(t, zero.IsClosed(), "zero multi-pool must report itself as OPENED")
	}
}

// TestMultiPoolValueCopyConsistency pins that copying a multi-pool value copies
// its core state instead of aliasing it, matching the semantics of the plain
// state fields the three types used to carry. This is why the shared core is
// embedded by value: a by-pointer embed would make the copy share one state.
func TestMultiPoolValueCopyConsistency(t *testing.T) {
	mp, err := ants.NewMultiPool(2, 1, ants.RoundRobin)
	require.NoError(t, err)
	require.False(t, mp.IsClosed())
	mpCopy := *mp
	require.NoError(t, mp.ReleaseTimeout(3*time.Second))
	require.True(t, mp.IsClosed(), "released original must be closed")
	require.False(t, mpCopy.IsClosed(), "value copy must not alias the original core state")

	mpFunc, err := ants.NewMultiPoolWithFunc(2, 1, func(any) {}, ants.RoundRobin)
	require.NoError(t, err)
	require.False(t, mpFunc.IsClosed())
	mpFuncCopy := *mpFunc
	require.NoError(t, mpFunc.ReleaseTimeout(3*time.Second))
	require.True(t, mpFunc.IsClosed(), "released original must be closed")
	require.False(t, mpFuncCopy.IsClosed(), "value copy must not alias the original core state")

	mpGeneric, err := ants.NewMultiPoolWithFuncGeneric(2, 1, func(chan struct{}) {}, ants.RoundRobin)
	require.NoError(t, err)
	require.False(t, mpGeneric.IsClosed())
	mpGenericCopy := *mpGeneric
	require.NoError(t, mpGeneric.ReleaseTimeout(3*time.Second))
	require.True(t, mpGeneric.IsClosed(), "released original must be closed")
	require.False(t, mpGenericCopy.IsClosed(), "value copy must not alias the original core state")
}

// TestMultiPoolRoutingLandingAnchored pins the absolute routing landing instead
// of only comparing entry points against each other. All three entry points
// share one core now, so a regression inside that core would be invisible to a
// pure cross-entry comparison: every entry point would agree while all of them
// drifted together. This is the exact failure mode reported for rolling
// restarts, where the round-robin landing point moved.
func TestMultiPoolRoutingLandingAnchored(t *testing.T) {
	const (
		size        = 3
		sizePerPool = 3
		submissions = 6
	)

	// With index starting at math.MaxUint32, the first RoundRobin increment
	// wraps to 0, so a fresh group starts its rotation at pool 0. Reboot()
	// resets index to 0, which makes the first post-reboot increment 1, so a
	// rebooted group starts its rotation at pool 1. That offset reset is the
	// documented rolling-restart behavior and must not change.
	wantFresh := []int{0, 1, 2, 0, 1, 2}
	wantRebooted := []int{1, 2, 0, 1, 2, 0}

	for _, ep := range multiPoolEntryPoints {
		ep := ep
		t.Run(ep.name, func(t *testing.T) {
			t.Parallel()

			mp, err := ep.open(size, sizePerPool, ants.RoundRobin)
			require.NoError(t, err)

			freshTrace, drainFresh := recordMultiPoolRouting(t, mp, size, submissions)
			drainFresh()
			require.Eventually(t, func() bool {
				return mp.Running() == 0
			}, 3*time.Second, 10*time.Millisecond)
			require.NoError(t, mp.ReleaseTimeout(3*time.Second))
			require.Equal(t, wantFresh, pickedPools(freshTrace, size), "%s fresh round-robin landing drifted", ep.name)

			mp.Reboot()
			rebootedTrace, drainRebooted := recordMultiPoolRouting(t, mp, size, submissions)
			drainRebooted()
			require.Eventually(t, func() bool {
				return mp.Running() == 0
			}, 3*time.Second, 10*time.Millisecond)
			require.NoError(t, mp.ReleaseTimeout(3*time.Second))
			require.Equal(t, wantRebooted, pickedPools(rebootedTrace, size), "%s rebooted round-robin landing drifted", ep.name)
		})
	}
}

// TestMultiPoolEntryPointsOverloadFallbackConsistency asserts that the
// RoundRobin overload fallback (re-dispatch with LeastTasks) behaves the same
// across the three entry points.
func TestMultiPoolEntryPointsOverloadFallbackConsistency(t *testing.T) {
	const (
		size        = 2
		sizePerPool = 1
	)

	type fallbackResult struct {
		loaded        multiPoolStat
		freed         multiPoolStat
		fallbackErr   string
		fallbackState multiPoolStat
		saturatedErr  string
	}

	run := func(ep multiPoolEntryPoint) fallbackResult {
		mp, err := ep.open(size, sizePerPool, ants.RoundRobin, ants.WithNonblocking(true))
		require.NoError(t, err)

		var result fallbackResult

		// Saturate both pools. The second submission lands on pool 1.
		firstBlocker := make(chan struct{})
		secondBlocker := make(chan struct{})
		require.NoError(t, mp.submit(firstBlocker))
		require.NoError(t, mp.submit(secondBlocker))
		result.loaded = snapshotMultiPool(t, mp, size)

		// Free pool 1's worker only; the next round-robin pick is pool 0,
		// which is still full, so Submit/Invoke must fall back to the least
		// loaded pool, i.e. pool 1.
		close(secondBlocker)
		require.Eventually(t, func() bool {
			running, err := mp.RunningByIndex(1)
			require.NoError(t, err)
			return running == 0
		}, 3*time.Second, 10*time.Millisecond)
		result.freed = snapshotMultiPool(t, mp, size)

		thirdBlocker := make(chan struct{})
		result.fallbackErr = multiPoolErrString(mp.submit(thirdBlocker))
		result.fallbackState = snapshotMultiPool(t, mp, size)

		// Both pools are full again and the fallback finds no headroom, so the
		// overload has to be reported as-is.
		result.saturatedErr = multiPoolErrString(mp.submit(make(chan struct{})))

		close(firstBlocker)
		close(thirdBlocker)
		require.Eventually(t, func() bool {
			return mp.Running() == 0
		}, 3*time.Second, 10*time.Millisecond)
		require.NoError(t, mp.ReleaseTimeout(3*time.Second))

		return result
	}

	base := run(multiPoolEntryPoints[0])
	for _, ep := range multiPoolEntryPoints[1:] {
		got := run(ep)
		require.Equal(t, base, got, "%s overload fallback diverged from %s", ep.name, multiPoolEntryPoints[0].name)
	}
}

// TestMultiPoolEntryPointsReleaseContextConsistency asserts that shutdown
// semantics, including an already-cancelled context and the resulting closed
// state, match across the three entry points.
func TestMultiPoolEntryPointsReleaseContextConsistency(t *testing.T) {
	const (
		size        = 2
		sizePerPool = 2
	)

	type releaseResult struct {
		cancelledErr  []string
		closedState   multiPoolStat
		closedErr     string
		afterReboot   multiPoolStat
		rebootSubmit  string
		cleanupErrStr string
	}

	run := func(ep multiPoolEntryPoint) releaseResult {
		mp, err := ep.open(size, sizePerPool, ants.RoundRobin)
		require.NoError(t, err)

		blocker := make(chan struct{})
		for i := 0; i < size*sizePerPool; i++ {
			require.NoError(t, mp.submit(blocker))
		}
		require.Eventually(t, func() bool {
			return mp.Running() == size*sizePerPool
		}, 3*time.Second, 10*time.Millisecond)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		var result releaseResult
		result.cancelledErr = multiPoolReleaseErrFragments(mp.ReleaseContext(ctx))
		result.closedState = snapshotMultiPool(t, mp, size)
		result.closedErr = multiPoolErrString(mp.ReleaseContext(context.Background()))

		close(blocker)
		require.Eventually(t, func() bool {
			return mp.Running() == 0
		}, 3*time.Second, 10*time.Millisecond)

		mp.Reboot()
		result.afterReboot = snapshotMultiPool(t, mp, size)
		rebootedBlocker := make(chan struct{})
		result.rebootSubmit = multiPoolErrString(mp.submit(rebootedBlocker))
		close(rebootedBlocker)
		require.Eventually(t, func() bool {
			return mp.Running() == 0
		}, 3*time.Second, 10*time.Millisecond)

		result.cleanupErrStr = multiPoolErrString(mp.ReleaseTimeout(3 * time.Second))
		return result
	}

	base := run(multiPoolEntryPoints[0])
	require.NotEmpty(t, base.cancelledErr, "a cancelled context must surface an error")
	require.Len(t, base.cancelledErr, size, "every pool in the group must report its own release failure")
	for _, ep := range multiPoolEntryPoints[1:] {
		got := run(ep)
		require.Equal(t, base, got, "%s release semantics diverged from %s", ep.name, multiPoolEntryPoints[0].name)
	}
}
