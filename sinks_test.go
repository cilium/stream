// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package stream_test

import (
	"context"
	"errors"
	"io"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	. "github.com/cilium/stream"
)

func TestFirst(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. First on non-empty source
	fst, err := First(ctx, Range(42, 1000))
	assertNil(t, "First", err)

	if fst != 42 {
		t.Fatalf("expected 42, got %d", fst)
	}

	// 2. First on empty source
	_, err = First(ctx, Empty[int]())
	if !errors.Is(err, io.EOF) {
		t.Fatalf("expected EOF, got %s", err)
	}

	// 3. cancelled context
	cancel()
	_, err = First(ctx, Stuck[int]())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected Canceled, got %s", err)
	}
}

func TestLast(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Last on non-empty source
	fst, err := Last(ctx, Range(42, 100))
	assertNil(t, "Last", err)

	if fst != 99 {
		t.Fatalf("expected 99, got %d", fst)
	}

	// 2. First on empty source
	_, err = Last(ctx, Empty[int]())
	if !errors.Is(err, io.EOF) {
		t.Fatalf("expected EOF, got %s", err)
	}

	// 3. cancelled context
	cancel()
	_, err = Last(ctx, Stuck[int]())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected Canceled, got %s", err)
	}
}

func TestToSlice(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	xs, err := ToSlice(ctx, Range(0, 5))
	assertNil(t, "ToSlice", err)
	assertSlice(t, "ToSlice", []int{0, 1, 2, 3, 4}, xs)
}

func TestToChannel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	test := func(bufSize int, withErrCh bool) {
		var errCh chan error
		if withErrCh {
			// Use unbuffered to make sure item channel is closed first.
			errCh = make(chan error)
			defer close(errCh)
		}
		nums := ToChannel(ctx, Range(0, 4), WithBufferSize(bufSize), WithErrorChan(errCh))

		if bufSize != 0 {
			// Check that the channel really gets buffered
			assert.Equal(t, reflect.ValueOf(nums).Cap(), bufSize)
		}

		s := []int{}
		for n := range nums {
			s = append(s, n)
		}
		assert.Equal(t, s, []int{0, 1, 2, 3})

		if errCh != nil {
			assert.NoError(t, <-errCh)
		}
	}

	test(0, false)
	test(10, false)
	test(0, true)
	test(10, true)
}

func TestToTruncatingChannel(t *testing.T) {
	testCases := []struct {
		name      string
		withErrCh bool
	}{
		{
			name:      "without error chan",
			withErrCh: false,
		},
		{
			name:      "with error chan",
			withErrCh: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			in := make(chan int)

			var errCh chan error
			if tc.withErrCh {
				// Use unbuffered to make sure item channel is closed first.
				errCh = make(chan error)
				defer close(errCh)
			}

			nums := ToTruncatingChannel(ctx, FromChannel(in), WithErrorChan(errCh))

			// each item from the source is observed
			for i := 0; i <= 5; i++ {
				in <- i
				item := <-nums
				assert.Equal(t, i, item)
			}

			// the observable is decoupled from the consumer
			for i := 5; i <= 10; i++ {
				in <- i
			}

			// simulate a busy consumer
			time.Sleep(5 * time.Millisecond)

			// when the consumer becomes available again, it observes the last item
			item := <-nums
			assert.Equal(t, 10, item)

			close(in)

			_, ok := <-nums
			assert.False(t, ok)

			if errCh != nil {
				assert.NoError(t, <-errCh)
			}
		})
	}
}

func TestToTruncatingChannelSlowConsumer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	in := make(chan struct{})
	defer close(in)

	mcast, connect := ToMulticast(FromChannel(in))
	src := ToTruncatingChannel(ctx, mcast)

	consumer := func(wg *sync.WaitGroup, d time.Duration, n *int) {
		defer wg.Done()

		for range src {
			select {
			case <-ctx.Done():
			case <-time.After(d):
				*n++
			}
		}
	}

	var wg sync.WaitGroup

	// slow consumer
	var slow int
	wg.Add(1)
	go consumer(&wg, time.Hour, &slow)

	// fast consumer
	var fast int
	wg.Add(1)
	go consumer(&wg, time.Millisecond, &fast)

	connect(ctx)

	timer := time.NewTimer(100 * time.Millisecond)
	defer timer.Stop()
stop:
	for {
		select {
		case <-timer.C:
			break stop
		case in <- struct{}{}:
		}
	}

	cancel()
	wg.Wait()

	// slow consumer should not slow down a fast consumer
	assert.Zero(t, slow)
	assert.Greater(t, fast, slow)
}
