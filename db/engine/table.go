package engine

import (
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/rrowniak/sqlparser/query"
)

func newTable(name string, sch schema) *table {
	return &table{tableLock: &sync.RWMutex{}, name: name, sch: sch}
}

type FieldType int

const (
	UNKNOWN_FIELD_TYPE FieldType = iota
	TEXT
	BOOL
	INT
	DATETIME
)

func fieldTypeFromString(s string) FieldType {
	switch strings.ToUpper(s) {
	case "TEXT":
		return TEXT
	case "BOOL":
		return BOOL
	case "INT":
		return INT
	case "DATETIME":
		return DATETIME
	default:
		return UNKNOWN_FIELD_TYPE
	}
}

type schema struct {
	name    []string
	colType []FieldType
}

type record struct {
	cells []string
}

type table struct {
	tableLock *sync.RWMutex
	name      string
	sch       schema
	records   []record
}

func (t *table) selectQ(query query.Query) (res QueryResult) {
	t.tableLock.RLock()
	defer t.tableLock.RUnlock()

	res.Status = "OK"
	err := t.validate(query)
	if err != nil {
		res.Err = err
		res.Status = "Schema error"
	}

	t.walkEvery(query.Conditions, func(r *record) {
		row := Row{Fields: make(map[string]string)}
		res.Rows = append(res.Rows, row)
		cells := &res.Rows[len(res.Rows)-1].Fields
		for _, f := range query.Fields {
			if f == "*" {
				for i, ff := range t.sch.name {
					(*cells)[ff] = r.cells[i]
				}
			} else {
				i := t.getFieldIndex(f)
				(*cells)[f] = r.cells[i]
			}
		}
	})

	return
}

func (t *table) updateQ(query query.Query) (res QueryResult) {
	t.tableLock.Lock()
	defer t.tableLock.Unlock()

	res.Status = "OK"
	err := t.validate(query)
	if err != nil {
		res.Err = err
		res.Status = "Schema error"
	}

	t.walkEvery(query.Conditions, func(r *record) {
		for f, v := range query.Updates {
			i := t.getFieldIndex(f)
			r.cells[i] = v
		}
	})

	return
}

func (t *table) insertQ(query query.Query) (res QueryResult) {
	t.tableLock.Lock()
	defer t.tableLock.Unlock()

	res.Status = "OK"
	err := t.validate(query)
	if err != nil {
		res.Err = err
		res.Status = "Schema error"
	}

	for _, ins := range query.Inserts {
		var rec record
		rec.cells = make([]string, len(t.sch.name))
		for i, val := range ins {
			rec.cells[t.getFieldIndex(query.Fields[i])] = val
		}
		t.records = append(t.records, rec)
	}
	return
}

func (t *table) deleteQ(query query.Query) (res QueryResult) {
	t.tableLock.Lock()
	defer t.tableLock.Unlock()

	res.Status = "OK"

	deleted := 0
	swap_cand := len(t.records) - 1
	for i := 0; i <= swap_cand; i++ {
		if t.evalConditions(query.Conditions, &t.records[i]) {
			// find a swap candidate
			deleted++
			found := false
			for j := swap_cand; j > i; j-- {
				if !t.evalConditions(query.Conditions, &t.records[j]) {
					swap_cand = j
					found = true
					break
				} else {
					deleted++
				}
			}

			if found {
				t.records[i] = t.records[swap_cand]
				swap_cand--
			} else {
				break
			}
		}
	}
	if deleted != 0 {
		t.records = t.records[:len(t.records)-deleted]
	}
	// TODO: either indexes have to be updated or tombstones should be leveraged
	return
}

func (t *table) dropQ() {

}

func (t *table) createIndexQ(query query.Query) (res QueryResult) {
	t.tableLock.RLock()
	defer t.tableLock.RUnlock()

	err := t.validate(query)
	if err != nil {
		res.Err = err
		res.Status = "Schema error"
	}

	return
}

func (t *table) getFieldIndex(f string) int {
	for i, sch_f := range t.sch.name {
		if f == sch_f {
			return i
		}
	}

	return -1
}

func (t *table) validate(query query.Query) error {
	for _, f := range query.Fields {
		if f == "*" {
			continue
		}
		if t.getFieldIndex(f) == -1 {
			return fmt.Errorf("schema violation: field %s not defined", f)
		}
	}

	return nil
}

func (t *table) evalCondition(cond query.Condition, r *record) bool {
	vals := make([]string, 2)
	var fieldTypes []FieldType

	if cond.Operand1IsField {
		indx := t.getFieldIndex(cond.Operand1)
		vals[0] = r.cells[indx]
		fieldTypes = append(fieldTypes, t.sch.colType[indx])
	} else {
		vals[0] = cond.Operand1
	}

	if cond.Operand2IsField {
		indx := t.getFieldIndex(cond.Operand2)
		vals[1] = r.cells[indx]
		fieldTypes = append(fieldTypes, t.sch.colType[indx])
	} else {
		vals[1] = cond.Operand2
	}

	if len(fieldTypes) == 0 {
		return false
	}

	if len(fieldTypes) == 2 && fieldTypes[0] != fieldTypes[1] {
		return false
	}

	switch fieldTypes[0] {
	case TEXT:
		switch cond.Operator {
		case query.Eq:
			return vals[0] == vals[1]
		case query.Ne:
			return vals[0] != vals[1]
		default:
			return false
		}
	case BOOL:
		v1 := vals[0] == "true"
		v2 := vals[1] == "true"
		switch cond.Operator {
		case query.Eq:
			return v1 == v2
		case query.Ne:
			return v1 != v2
		default:
			return false
		}
	case INT:
		v1, e := strconv.Atoi(vals[0])
		if e != nil {
			return false
		}
		v2, e := strconv.Atoi(vals[1])
		if e != nil {
			return false
		}
		switch cond.Operator {
		case query.Eq:
			return v1 == v2
		case query.Ne:
			return v1 != v2
		case query.Gt:
			return v1 > v2
		case query.Lt:
			return v1 < v2
		case query.Gte:
			return v1 >= v2
		case query.Lte:
			return v1 <= v2
		default:
			return false
		}
	case DATETIME:
		return false
	}

	return false
}

func (t *table) evalConditions(conds []query.Condition, r *record) bool {
	for _, cond := range conds {
		if !t.evalCondition(cond, r) {
			return false
		}
	}
	return true
}

func (t *table) walkEvery(conds []query.Condition, visitor func(r *record)) {
	for i := range t.records {
		if t.evalConditions(conds, &t.records[i]) {
			visitor(&t.records[i])
		}
	}
}
