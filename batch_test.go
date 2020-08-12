// Copyright 2020 VMware, Inc.
// SPDX-License-Identifier: BSD-2-Clause

package batchelor_test

import (
	"log"
	"sync"
	"testing"
	"time"

	"github.com/mkmik/batchelor"
)

type heavyLifting struct {
	id int
}

func (h *heavyLifting) Process(ops []func() error) error {
	log.Printf("preparing %d", h.id)
	time.Sleep(100 * time.Millisecond)
	log.Printf("prepared %d", h.id)

	for _, o := range ops {
		if err := o(); err != nil {
			return err
		}
	}

	log.Printf("committing %d", h.id)
	time.Sleep(100 * time.Millisecond)
	log.Printf("commited %d", h.id)
	return nil
}

func TestBatch(t *testing.T) {
	n := 0
	var mu sync.Mutex
	newJob := func() batchelor.Batch { mu.Lock(); defer mu.Unlock(); n++; return &heavyLifting{id: n} }
	q := batchelor.NewQueue(newJob)

	errChs := make([]<-chan error, 3)
	for i := 0; i < len(errChs); i++ {
		errChs[i] = q.DoChan(func() error { log.Printf("do %d", i); return nil })
	}
	for _, errCh := range errChs {
		if err := <-errCh; err != nil {
			t.Error(err)
		}
	}
}
