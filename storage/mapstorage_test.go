package storage

import (
  "testing"
)


func TestSetAndGet(t *testing.T) {

  storage := newMapStorage()

  storage.Set("foo", 0, 60, 5, []byte("babab"))
  flag, bytes, _, content, err := storage.Get("foo")

  assertEquals(t, int(flag), 0, "invalid flag")
  assertEquals(t, int(bytes), 5, "invalid byte lenght")
  assertEquals(t, string(content), "babab", "invalid content")
  assertEquals(t, err, nil, "Invalid err ")
}

func TestSetShouldUpdateCas(t *testing.T) {

  storage := newMapStorage()

  storage.Set("foo", 0, 60, 5, []byte("aaaaa"))
  _, _, cas_before, _, _ := storage.Get("foo")
  storage.Set("foo", 0, 60, 5, []byte("bbbbb"))
  _, _, cas_after, _, _ := storage.Get("foo")

  assertNotEquals(t, cas_before, cas_after, "Invalid cas update")

}


func TestAddShouldFailIfKeyAlreadyExists(t *testing.T) {

  storage := newMapStorage()

  storage.Set("foo", 0, 60, 5, []byte("aaaaa"))
  err := storage.Add("foo", 1, 30, 4, []byte("bbbb"))

  assertNotEquals(t, err, nil, "failed to add")
}

func TestAddShouldAddIfNotExists(t *testing.T) {

  storage := newMapStorage()

  storage.Set("foo", 0, 60, 5, []byte("aaaaa"))
  err := storage.Add("bar", 1, 30, 4, []byte("bbbb"))

  assertEquals(t, err, nil, "failed to add")
}


func TestShouldReplaceIfExists(t *testing.T) {

  storage := newMapStorage()
  
  storage.Set("foo", 0, 60, 5, []byte("aaaaa"))
  err := storage.Replace("foo", 1, 120, 4, []byte("bbbb"))

  flag, bytes, _, content, err := storage.Get("foo") 

  assertEquals(t, int(flag), 1, "invalid flag")
  assertEquals(t, int(bytes), 4, "invalid byte lenght")
  assertEquals(t, string(content), "bbbb", "invalid content")
  assertEquals(t, err, nil, "Invalid err ")
}


func TestReplaceShouldFailIfKeyNotExists(t *testing.T) {

  storage := newMapStorage()
  
  err := storage.Replace("foo", 0, 60, 4, []byte("aaaa"))

  assertNotEquals(t, err, nil, "invalid error")
}

func TestShouldAppendContentForKey(t *testing.T) {

  storage := newMapStorage()
  
  storage.Set("foo", 0, 60, 5, []byte("aaaaa"))
  err := storage.Append("foo", 4, []byte("bbbb"))

  flag, bytes, _, content, err := storage.Get("foo") 

  assertEquals(t, int(flag), 0, "invalid flag")
  assertEquals(t, int(bytes), 9, "invalid byte lenght")
  assertEquals(t, string(content), "aaaaabbbb", "invalid content")
  assertEquals(t, err, nil, "Invalid err ")
}


func TestShouldPrependContentForKey(t *testing.T) {

  storage := newMapStorage()
  
  storage.Set("foo", 0, 60, 5, []byte("aaaaa"))
  err := storage.Prepend("foo", 4, []byte("bbbb"))

  flag, bytes, _, content, err := storage.Get("foo") 

  assertEquals(t, int(flag), 0, "invalid flag")
  assertEquals(t, int(bytes), 9, "invalid byte lenght")
  assertEquals(t, string(content), "bbbbaaaaa", "invalid content")
  assertEquals(t, err, nil, "Invalid err ")
}

func assertEquals(t *testing.T, a interface{}, b interface{}, cause string) {
  if a != b {
    t.Error(cause);
  }
}

func assertNotEquals(t *testing.T, a interface{}, b interface{}, cause string) {
  if a == b {
    t.Error(cause);
  }
}
