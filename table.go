package memtable

import (
	"encoding/json"
	"errors"
	"sync"
)

type Table[K comparable, V any] struct {
	rwlock sync.RWMutex
	mRow   map[K]V
}

func CreateTable[K comparable, V any]() *Table[K, V] {
	return &Table[K, V]{
		rwlock: sync.RWMutex{},
		mRow:   map[K]V{},
	}
}

func (t *Table[K, V]) Insert(mainkey K, row V) {
	t.rwlock.Lock()
	defer t.rwlock.Unlock()

	t.mRow[mainkey] = row
}

func (t *Table[K, V]) SelectByMainKey(mainkey K) (V, bool) {
	t.rwlock.RLock()
	defer t.rwlock.RUnlock()

	v, ok := t.mRow[mainkey]
	return v, ok
}

func (t *Table[K, V]) SelectOneByCondition(cond func(row V) bool) (V, error) {
	t.rwlock.RLock()
	defer t.rwlock.RUnlock()

	var result V
	for _, row := range t.mRow {
		if cond(row) {
			return row, nil
		}
	}

	return result, errors.New("not find...")
}

func (t *Table[K, V]) SelectAndUpdateOnce(cond func(row V) bool, update func(row V) V) (K, bool) {
	t.rwlock.Lock()
	defer t.rwlock.Unlock()

	var result K
	var bup = false
	for mainkey, row := range t.mRow {
		if cond(row) {
			result = mainkey
			t.mRow[mainkey] = update(row)
			bup = true
			break
		}
	}

	return result, bup
}

func (t *Table[K, V]) SelectByCondition(cond func(row V) bool) []V {
	t.rwlock.RLock()
	defer t.rwlock.RUnlock()

	results := make([]V, 0, len(t.mRow))
	for _, row := range t.mRow {
		if cond(row) {
			results = append(results, row)
		}
	}

	return results
}

func (t *Table[K, V]) UpdateByMainkey(mainkey K, cond func(row V) V) error {
	t.rwlock.Lock()
	defer t.rwlock.Unlock()

	row, ok := t.mRow[mainkey]

	if !ok {
		return errors.New("row not find...")
	}
	t.mRow[mainkey] = cond(row)

	return nil
}

func (t *Table[K, V]) UpdateByCondition(cond func(row V) V) error {
	t.rwlock.Lock()
	defer t.rwlock.Unlock()

	for key, row := range t.mRow {
		t.mRow[key] = cond(row)
	}

	return nil
}

func (t *Table[K, V]) DelByMainkey(mainkey K) (V, bool) {
	t.rwlock.Lock()
	defer t.rwlock.Unlock()

	temp, ok := t.mRow[mainkey]

	if ok {
		delete(t.mRow, mainkey)
	}

	return temp, ok
}

func (t *Table[K, V]) DelByCondition(cond func(row V) bool) []V {
	t.rwlock.Lock()
	defer t.rwlock.Unlock()

	results := make([]V, 0, len(t.mRow))
	for key, row := range t.mRow {
		if cond(row) {
			results = append(results, row)
			delete(t.mRow, key)
		}
	}

	return results
}

func (t *Table[K, V]) GetJsonData() string {
	t.rwlock.Lock()
	defer t.rwlock.Unlock()

	temp, _ := json.Marshal(t.mRow)

	return string(temp)
}
