package memtable

import (
	"errors"
	"sync"
)

type Table struct {
	rwlock sync.RWMutex
	mRow   map[string]any
}

func CreateTable() *Table {
	return &Table{
		rwlock: sync.RWMutex{},
		mRow:   map[string]any{},
	}
}

func (t *Table) Insert(mainkey string, row any) {
	t.rwlock.Lock()
	defer t.rwlock.Unlock()

	t.mRow[mainkey] = row
}

func (t *Table) SelectByMainKey(mainkey string) any {
	t.rwlock.RLock()
	defer t.rwlock.RUnlock()
	return t.mRow[mainkey]
}

func (t *Table) SelectByCondition(cond func(row any) bool) []any {
	t.rwlock.RLock()
	defer t.rwlock.RUnlock()

	results := make([]any, 0, len(t.mRow))
	for _, row := range t.mRow {
		if cond(row) {
			temp := row
			results = append(results, temp)
		}
	}

	return results
}

func (t *Table) UpdateByMainkey(mainkey string, cond func(row any) any) error {
	t.rwlock.Lock()
	defer t.rwlock.Unlock()

	row := t.mRow[mainkey]

	if row == nil {
		return errors.New("row not find...")
	}
	t.mRow[mainkey] = cond(row)

	return nil
}

func (t *Table) UpdateByCondition(cond func(row any) any) error {
	t.rwlock.Lock()
	defer t.rwlock.Unlock()

	for key, row := range t.mRow {
		t.mRow[key] = cond(row)
	}

	return nil
}

func (t *Table) DelByMainkey(mainkey string) {
	t.rwlock.Lock()
	defer t.rwlock.Unlock()
	delete(t.mRow, mainkey)
	return
}

func (t *Table) DelByCondition(cond func(row any) bool) {
	t.rwlock.Lock()
	defer t.rwlock.Unlock()

	for key, row := range t.mRow {
		if cond(row) {
			delete(t.mRow, key)
		}
	}

	return
}
