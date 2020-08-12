// Copyright 2020 VMware, Inc.
// SPDX-License-Identifier: BSD-2-Clause

package batchelor_test

import (
	"fmt"
	"log"
	"sync"
	"testing"

	"github.com/mkmik/batchelor"
)

type heavyLifting struct {
	id int
}

func (h *heavyLifting) Process(ops []func() error) error {
	log.Printf("preparing %d", h.id)

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
	var mu sync.Mutex
	newJob := func() batchelor.Batch {
		mu.Lock()
		defer mu.Unlock()

		n++
		return &heavyLifting{id: n}
	}
	q := batchelor.NewQueue(newJob)

	for j := 0; j < 2; j++ {
		w := make(chan struct{})

		errChs := make([]<-chan error, 4)
		for i := 0; i < len(errChs); i++ {
			i := i
			errChs[i] = q.DoChan(func() error {
				mu.Lock()
				defer mu.Unlock()
				<-w
				log.Printf("do %d", i)
				return nil
			})
		}

		close(w)

		for _, errCh := range errChs {
			if err := <-errCh; err != nil {
				t.Error(err)
			}
		}
	}
}

func TestError(t *testing.T) {
	n := 0
	var mu sync.Mutex
	newJob := func() batchelor.Batch {
		mu.Lock()
		defer mu.Unlock()

		n++
		return &heavyLifting{id: n}
	}
	q := batchelor.NewQueue(newJob)

	w := make(chan struct{})

	errChs := make([]<-chan error, 4)
	for i := 0; i < len(errChs); i++ {
		i := i
		errChs[i] = q.DoChan(func() error {
			mu.Lock()
			defer mu.Unlock()
			<-w
			log.Printf("do %d", i)
			if i == 2 {
				return fmt.Errorf("test error %d", i)
			}
			return nil
		})
	}

	close(w)

	for i, errCh := range errChs {
		err := <-errCh
		// operation 2 runs in the second batch, along with operations 1 and 3, which
		// all fail since the whole batch fails because of operation 2.
		if i == 0 && err != nil {
			t.Error(err)
		} else if i > 0 {
			if got, want := err.Error(), "test error 2"; got != want {
				t.Errorf("expecting %q got %q", want, err)
			}
		}
	}
}
