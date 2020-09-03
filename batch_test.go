// Copyright 2020 VMware, Inc.
// SPDX-License-Identifier: BSD-2-Clause

package batchelor_test

import (
	"fmt"
	"log"
	"testing"

	"github.com/mkmik/batchelor"
)

type heavyLifting struct {
	id int
	// waiter signals the Prepare function that it can proceed
	waiter <-chan struct{}
}

func (h *heavyLifting) Prepare() error {
	<-h.waiter
	log.Printf("preparing %d", h.id)
	return nil
}

func (h *heavyLifting) Process(ops []func() error) error {
	for _, o := range ops {
		if err := o(); err != nil {
			return err
		}
	}

	log.Printf("commited %d", h.id)
	return nil
}

func TestBatch(t *testing.T) {
	n := 0

	w := make(chan struct{})
	defer close(w)

	newJob := func() batchelor.Batch {
		n++
		return &heavyLifting{id: n, waiter: w}
	}
	q := batchelor.NewQueue(newJob)

	for j := 0; j < 2; j++ {
		errChs := make([]<-chan error, 4)
		for i := 0; i < len(errChs); i++ {
			i := i
			errChs[i] = q.DoChan(func() error {
				log.Printf("do %d", i)
				return nil
			})
		}

		w <- struct{}{}

		for _, errCh := range errChs {
			if err := <-errCh; err != nil {
				t.Error(err)
			}
		}
	}
}

func TestError(t *testing.T) {
	n := 0

	w := make(chan struct{})
	defer close(w)

	newJob := func() batchelor.Batch {
		n++
		return &heavyLifting{id: n, waiter: w}
	}
	q := batchelor.NewQueue(newJob)

	errChs := make([]<-chan error, 4)
	for i := 0; i < len(errChs); i++ {
		i := i
		errChs[i] = q.DoChan(func() error {
			log.Printf("do %d", i)
			if i == 2 {
				return fmt.Errorf("test error %d", i)
			}
			return nil
		})
	}

	w <- struct{}{}

	for _, errCh := range errChs {
		err := <-errCh
		if got, want := err.Error(), "test error 2"; got != want {
			t.Errorf("expecting %q got %q", want, err)
		}
	}
}

// TestFirstBatch ensures the initial batch accepts multiple items
func TestFirstBatch(t *testing.T) {
	n := 0

	w := make(chan struct{})
	defer close(w)

	newJob := func() batchelor.Batch {
		n++
		return &heavyLifting{id: n, waiter: w}
	}
	q := batchelor.NewQueue(newJob)

	for j := 1; j < 4; j++ {
		opsPerBatch := j * 2
		var processed int
		errChs := make([]<-chan error, opsPerBatch)
		for i := 0; i < len(errChs); i++ {
			i := i
			errChs[i] = q.DoChan(func() error {
				processed++
				log.Printf("do %d", i)
				return nil
			})
		}

		w <- struct{}{}

		for _, errCh := range errChs {
			if err := <-errCh; err != nil {
				t.Error(err)
			}
		}
		if got, want := processed, opsPerBatch; got != want {
			t.Errorf("expecting %d, got %d", want, got)
		}
	}
}
