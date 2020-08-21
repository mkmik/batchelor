// Copyright 2020 VMware, Inc.
// SPDX-License-Identifier: BSD-2-Clause

// Package batchelor implements a generic batching executor. The API is loosely modeled golang.org/x/sync/singleflight.
package batchelor

import "sync"

// A Batch is a generic unit of work composed of a number of operations that will be executed
// together.
type Batch interface {
	Prepare() error
	Process(ops []func() error) error
}

// A Queue executes operations in batches. While a batch is executing, all new incoming
// operations are added to a new "inbox" batch. When the current inflight batch finishes executing
// the inbox batch is executed and new operations will be queued in the next pending batch.
type Queue struct {
	newBatch func() Batch

	mu  sync.Mutex // protects in, out
	in  *job       // inbox (pending) batch.
	out *job       // outbox (inflight) batch.
}

// NewQueue creates a new queue given a batch construction function.
func NewQueue(newBatch func() Batch) *Queue {
	return &Queue{newBatch: newBatch}
}

// Do executes an operation.
// The call blocks until the operation has been actually executed.
func (q *Queue) Do(op func() error) error {
	return <-q.DoChan(op)
}

// DoChan executes an operation. Once the operation executes, the return value will be sent to the returned channel.
func (q *Queue) DoChan(op func() error) <-chan error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.in == nil {
		q.in = newJob(q.newBatch())
	}
	b := q.in

	done := b.add(op)
	q.try()

	ch := make(chan error, 1)
	go func() {
		<-done
		ch <- b.err
	}()
	return ch
}

// to be called while the mutex is held
func (q *Queue) try() {
	if q.in != nil && q.out == nil {
		q.in, q.out = nil, q.in
		go func(b *job) {
			b.commit()

			q.mu.Lock()
			defer q.mu.Unlock()
			q.out = nil
			q.try()
		}(q.out)
	}
}

type job struct {
	batch Batch
	ops   []func() error
	err   error
	done  chan struct{}
}

func newJob(batch Batch) *job {
	return &job{
		batch: batch,
		done:  make(chan struct{}),
	}
}

func (b *job) add(op func() error) <-chan struct{} {
	b.ops = append(b.ops, op)
	return b.done
}

func (b *job) commit() {
	defer close(b.done)

	if err := b.batch.Prepare(); err != nil {
		b.err = err
		return
	}
	b.err = b.batch.Process(b.ops)
}
