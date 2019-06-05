package client

import "sync"

type Blocker struct {
	mt      sync.Mutex
	wg      sync.WaitGroup
	blocked bool
}

func (b *Blocker) Block() {
	b.mt.Lock()
	if !b.blocked {
		b.wg.Add(1)
		b.blocked = true
	}
	b.mt.Unlock()
	b.wg.Wait()
}

func (b *Blocker) Unblock() {
	b.mt.Lock()
	defer b.mt.Unlock()
	if b.blocked {
		b.wg.Done()
		b.blocked = false
	}
}
