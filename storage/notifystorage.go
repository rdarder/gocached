package storage

import (
	"expiry"
	"time"
	"container/heap"
	"os"
	"log"
)

var logger = log.New(os.Stdout, "gocached [storage]: ", log.Lshortfile|log.LstdFlags)

type NotifyStorage struct {
	MapStorage
	bg   chan expiry.HeapEntry
	heap *expiry.Heap
}

func (ns NotifyStorage) ExpiringDaemon(freq int64) {
	logger.Println("Collecting")
	for timer := time.NewTicker(freq * 1000000); ; {
		select {
		case <-timer.C:
			logger.Println("Collecting expired entries")
			ns.Collect()
		case entry := <-ns.bg:
			logger.Println("Updating heap")
			ns.Update(entry)
		}
	}
	logger.Println("Exit Expiring Daemon")
}

func (ns NotifyStorage) Update(entry expiry.HeapEntry) {
	now := uint32(time.Seconds())
	if entry.Exptime > now {
		logger.Println("saving entry in heap")
		heap.Push(ns.heap, entry)
	}
}

func (ns NotifyStorage) Collect() {
	now := uint32(time.Seconds())
	h := ns.heap
	if h.Len() == 0 {
		logger.Println("No elements in expiry heap")
		return
	}
	logger.Printf("heap size: %v. heap: %v", h.Len(), *h)
	for h.Len() > 0 {
		tip := h.Tip()
		if tip.Exptime > now {
			break
		}
		h.Pop()
		logger.Println("trying to expire %+v at %v", tip, now)
		ns.MaybeExpire(*tip.Key, now)
	}
}

func (ns *NotifyStorage) Init(daemon_freq int64) {
	logger.Println("init notify storage")
	ns.MapStorage.Init()
	ns.bg = make(chan expiry.HeapEntry, 100)
	ns.heap = expiry.NewHeap(100)
	go ns.ExpiringDaemon(daemon_freq)
}

func (self *NotifyStorage) Set(key string, flags uint32, exptime uint32, bytes uint32, content []byte) (err os.Error) {
	err = self.MapStorage.Set(key, flags, exptime, bytes, content)
	if err == nil {
		logger.Println("sending to notify channel")
		self.bg <- expiry.HeapEntry{&key, exptime}
	}
	return err
}

func (self *NotifyStorage) Add(key string, flags uint32, exptime uint32, bytes uint32, content []byte) (err os.Error) {
	err = self.MapStorage.Add(key, flags, exptime, bytes, content)
	if err == nil {
		self.bg <- expiry.HeapEntry{&key, exptime}
	}
	return err
}

func (self *NotifyStorage) Replace(key string, flags uint32, exptime uint32, bytes uint32, content []byte) (err os.Error) {
	err = self.MapStorage.Replace(key, flags, exptime, bytes, content)
	if err == nil {
		self.bg <- expiry.HeapEntry{&key, exptime}
	}
	return err
}

func (self *NotifyStorage) Cas(key string, flags uint32, exptime uint32, bytes uint32, cas_unique uint64, content []byte) (err os.Error) {
	err = self.MapStorage.Cas(key, flags, exptime, bytes, cas_unique, content)
	if err == nil {
		self.bg <- expiry.HeapEntry{&key, exptime}
	}
	return err
}
