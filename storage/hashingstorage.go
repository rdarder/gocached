package storage

import (
	"os"
)

type Hasher func(string) uint32

type HashingStorage struct {
	size           uint32
	hasher         Hasher
	storageBuckets []NotifyStorage
}

/*
func NewHashingStorage(size uint32) *HashingStorage {
  h := &HashingStorage{}
  h.Init(size)
  return h
}
*/

func (h HashingStorage) Init(size uint32) {
	h.size = size
	h.hasher = hornerHasher
	h.storageBuckets = make([]NotifyStorage, size)

	for i := uint32(0); i < size; i++ {
		h.storageBuckets[i].Init(5000)
	}
}

func (self *HashingStorage) Set(key string, flags uint32, exptime uint32, bytes uint32, content []byte) (err os.Error) {
	return self.findBucket(key).Set(key, flags, exptime, bytes, content)
}

func (self *HashingStorage) Add(key string, flags uint32, exptime uint32, bytes uint32, content []byte) (err os.Error) {
	return self.findBucket(key).Add(key, flags, exptime, bytes, content)
}

func (self *HashingStorage) Replace(key string, flags uint32, exptime uint32, bytes uint32, content []byte) (err os.Error) {
	return self.findBucket(key).Replace(key, flags, exptime, bytes, content)
}

func (self *HashingStorage) Append(key string, bytes uint32, content []byte) (err os.Error) {
	return self.findBucket(key).Append(key, bytes, content)
}

func (self *HashingStorage) Prepend(key string, bytes uint32, content []byte) (err os.Error) {
	return self.findBucket(key).Prepend(key, bytes, content)
}

func (self *HashingStorage) Cas(key string, flags uint32, exptime uint32, bytes uint32, cas_unique uint64, content []byte) (err os.Error) {
	return self.findBucket(key).Cas(key, flags, exptime, bytes, cas_unique, content)
}

func (self *HashingStorage) Delete(key string) (flags uint32, bytes uint32, cas_unique uint64, content []byte, err os.Error) {
	return self.findBucket(key).Delete(key)
}

func (self *HashingStorage) Incr(key string, value uint64, incr bool) (resultValue uint64, err os.Error) {
	return self.findBucket(key).Incr(key, value, incr)
}

func (self *HashingStorage) Get(key string) (flags uint32, bytes uint32, cas_unique uint64, content []byte, err os.Error) {
	return self.findBucket(key).Get(key)
}

func (self *HashingStorage) MaybeExpire(key string, now uint32) bool {
	return self.findBucket(key).MaybeExpire(key, now)
}

func (self *HashingStorage) findBucket(key string) *NotifyStorage {
	return &self.storageBuckets[self.hasher(key)%self.size]
}

var hornerHasher = func(value string) uint32 {
	var hashcode uint32 = 1
	for i := 0; i < len(value); i++ {
		hashcode += (hashcode * 31) + uint32(value[i])
	}
	return hashcode
}
