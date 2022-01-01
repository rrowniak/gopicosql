package engine

import (
	"fmt"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/rrowniak/sqlparser/query"
)

type ftTestCase struct {
	name string
	exp  FieldType
	arg  string
}

func TestFieldTypesFromString(t *testing.T) {
	ts := []ftTestCase{
		{name: "unknown", exp: UNKNOWN_FIELD_TYPE, arg: "UNKNOWN_FIELD_TYPE"},
		{name: "text", exp: TEXT, arg: "TEXT"},
		{name: "bool", exp: BOOL, arg: "BOOL"},
		{name: "int", exp: INT, arg: "INT"},
		{name: "datetime", exp: DATETIME, arg: "DATETIME"},
	}

	for _, tc := range ts {
		t.Run(tc.name, func(t *testing.T) {
			ret := fieldTypeFromString(tc.arg)
			if ret != tc.exp {
				t.Errorf("Expected %d, got %d", tc.exp, ret)
			}
		})
	}
}

func checkQueryOk(qr QueryResult) error {
	if qr.Err != nil {
		return fmt.Errorf("Error is not expected, got %s", qr.Err)
	}

	if qr.Status != "OK" {
		return fmt.Errorf("Expected status 'OK', got '%s'", qr.Status)
	}

	return nil
}

func TextNewEmptyTable(t *testing.T) {
	tn := "NewTable"
	sch := schema{name: []string{"id"}, colType: []FieldType{INT}}
	table := newTable(tn, sch)

	q := query.Query{Type: query.Select, TableName: tn, Fields: []string{"*"}}
	qr := table.selectQ(q)

	if e := checkQueryOk(qr); e != nil {
		t.Errorf(e.Error())
	}

	if len(qr.Rows) != 0 {
		t.Errorf("Expected now rows, got %d rows", len(qr.Rows))
	}
}

func TestInsertRows(t *testing.T) {
	tn := "NewTable"
	sch := schema{name: []string{"id", "val"}, colType: []FieldType{INT, TEXT}}
	table := newTable(tn, sch)

	for i := 1; i <= 100; i++ {
		istr := strconv.Itoa(i)
		// Insert element
		q := query.Query{
			Type:      query.Insert,
			TableName: tn,
			Fields:    []string{"id", "val"},
			Inserts:   [][]string{{istr, istr}},
		}

		qr := table.insertQ(q)
		if e := checkQueryOk(qr); e != nil {
			t.Errorf(e.Error())
		}

		// check if insert works #1
		q = query.Query{Type: query.Select, TableName: tn, Fields: []string{"*"}}
		qr = table.selectQ(q)
		if e := checkQueryOk(qr); e != nil {
			t.Errorf(e.Error())
		}

		if len(qr.Rows) != i {
			t.Errorf("Expected %d rows, got %d rows", i, len(qr.Rows))
		}

		// check if insert works #2
		// SELECT id, val FROM $TableName WHERE id = '$i' AND val != '0'
		q = query.Query{
			Type:      query.Select,
			TableName: tn,
			Fields:    []string{"id", "val"},
			Conditions: []query.Condition{
				{Operand1: "id", Operand1IsField: true, Operator: query.Eq, Operand2: istr, Operand2IsField: false},
				{Operand1: "val", Operand1IsField: true, Operator: query.Ne, Operand2: "0", Operand2IsField: false},
			},
		}
		qr = table.selectQ(q)
		if e := checkQueryOk(qr); e != nil {
			t.Errorf(e.Error())
		}

		if len(qr.Rows) != 1 {
			t.Errorf("Expected 1 row, got %d rows", len(qr.Rows))
		} else {
			if len(qr.Rows[0].Fields) != 2 {
				t.Errorf("Expected 2 fields, got %v", qr.Rows[0].Fields)
			}
			for _, f := range []string{"id", "val"} {
				id, ok := qr.Rows[0].Fields[f]
				if !ok {
					t.Errorf("Expected '%s' field as a restul of query execution", f)
				} else if id != istr {
					t.Errorf("Expected %s == '%s', got %s == '%s'", f, istr, f, id)
				}
			}
		}

	}
}

func TestUpdateRows(t *testing.T) {
	tn := "NewTable"
	sch := schema{name: []string{"id", "val"}, colType: []FieldType{INT, TEXT}}
	table := newTable(tn, sch)

	// insert N rows
	for i := 1; i <= 100; i++ {
		istr := strconv.Itoa(i + 100)
		// Insert element
		q := query.Query{
			Type:      query.Insert,
			TableName: tn,
			Fields:    []string{"id", "val"},
			Inserts:   [][]string{{istr, istr}},
		}

		qr := table.insertQ(q)
		if e := checkQueryOk(qr); e != nil {
			t.Errorf(e.Error())
		}
	}

	// update those rows
	for i := 1; i <= 100; i++ {
		old_istr := strconv.Itoa(i + 100)
		istr := strconv.Itoa(i)
		q := query.Query{
			Type:      query.Update,
			TableName: tn,
			Updates:   map[string]string{"id": istr, "val": istr},
			Conditions: []query.Condition{
				{Operand1: "id", Operand1IsField: true, Operator: query.Eq, Operand2: old_istr, Operand2IsField: false},
			},
		}
		qr := table.updateQ(q)
		if e := checkQueryOk(qr); e != nil {
			t.Errorf(e.Error())
		}
	}

	// check if update works
	for i := 1; i <= 100; i++ {
		istr := strconv.Itoa(i)
		// check if update works
		// SELECT id, val FROM $TableName WHERE id = '$i' AND val != '0'
		q := query.Query{
			Type:      query.Select,
			TableName: tn,
			Fields:    []string{"id", "val"},
			Conditions: []query.Condition{
				{Operand1: "id", Operand1IsField: true, Operator: query.Eq, Operand2: istr, Operand2IsField: false},
				{Operand1: "val", Operand1IsField: true, Operator: query.Ne, Operand2: "0", Operand2IsField: false},
			},
		}
		qr := table.selectQ(q)
		if e := checkQueryOk(qr); e != nil {
			t.Errorf(e.Error())
		}

		if len(qr.Rows) != 1 {
			t.Errorf("Expected 1 row, got %d rows", len(qr.Rows))
		} else {
			if len(qr.Rows[0].Fields) != 2 {
				t.Errorf("Expected 2 fields, got %v", qr.Rows[0].Fields)
			}
			for _, f := range []string{"id", "val"} {
				id, ok := qr.Rows[0].Fields[f]
				if !ok {
					t.Errorf("Expected '%s' field as a restul of query execution", f)
				} else if id != istr {
					t.Errorf("Expected %s == '%s', got %s == '%s'", f, istr, f, id)
				}
			}
		}

	}
}

func TestDeleteRows(t *testing.T) {
	tn := "NewTable"
	sch := schema{name: []string{"id", "val"}, colType: []FieldType{INT, TEXT}}
	table := newTable(tn, sch)

	// insert N rows
	for i := 1; i <= 100; i++ {
		istr := strconv.Itoa(i)
		// Insert element
		q := query.Query{
			Type:      query.Insert,
			TableName: tn,
			Fields:    []string{"id", "val"},
			Inserts:   [][]string{{istr, istr}},
		}

		qr := table.insertQ(q)
		if e := checkQueryOk(qr); e != nil {
			t.Errorf(e.Error())
		}
	}

	// delete those rows
	for i := 1; i <= 100; i++ {
		istr := strconv.Itoa(i)
		q := query.Query{
			Type:      query.Delete,
			TableName: tn,
			Conditions: []query.Condition{
				{Operand1: "val", Operand1IsField: true, Operator: query.Eq, Operand2: istr, Operand2IsField: false},
			},
		}
		qr := table.deleteQ(q)
		if e := checkQueryOk(qr); e != nil {
			t.Errorf(e.Error())
		}

		// check if delete works
		q = query.Query{
			Type:      query.Select,
			TableName: tn,
			Fields:    []string{"id", "val"},
			Conditions: []query.Condition{
				{Operand1: "val", Operand1IsField: true, Operator: query.Ne, Operand2: istr, Operand2IsField: false},
			},
		}
		qr = table.selectQ(q)
		if e := checkQueryOk(qr); e != nil {
			t.Errorf(e.Error())
		}

		if len(qr.Rows) != 100-i {
			t.Errorf("Expected %d rows, got %d rows", 100-i, len(qr.Rows))
		}
	}
}

func TestDeleteMultipleRows(t *testing.T) {
	const N = 100
	tn := "NewTable"
	sch := schema{name: []string{"id", "val", "updated"}, colType: []FieldType{INT, TEXT, BOOL}}
	table := newTable(tn, sch)

	for i := N - 1; i >= 0; i-- {
		q := query.Query{
			Type:      query.Insert,
			TableName: tn,
			Fields:    []string{"id", "val", "updated"},
			Inserts:   [][]string{{strconv.Itoa(i), strconv.Itoa(i), "false"}},
		}

		table.insertQ(q)
	}

	// delete half of the table
	q := query.Query{
		Type:      query.Delete,
		TableName: tn,
		Conditions: []query.Condition{
			{Operand1: "id", Operand1IsField: true, Operator: query.Lt, Operand2: strconv.Itoa(N / 2), Operand2IsField: false},
		},
	}
	// double delete
	table.deleteQ(q)
	table.deleteQ(q)

	// we should have only N/2 records, let's check that out
	q = query.Query{
		Type:      query.Select,
		TableName: tn,
		Fields:    []string{"*"},
	}
	qr := table.selectQ(q)
	expected := N - N/2

	if len(qr.Rows) != expected {
		t.Errorf("Expected %d rows, got %d rows", expected, len(qr.Rows))
	}
}

func TestDeleteMultipleRows2(t *testing.T) {
	const N = 100
	tn := "NewTable"
	sch := schema{name: []string{"id", "val", "updated"}, colType: []FieldType{INT, TEXT, BOOL}}
	table := newTable(tn, sch)

	for i, j := 0, N-1; i < j; i, j = i+1, j-1 {
		q := query.Query{
			Type:      query.Insert,
			TableName: tn,
			Fields:    []string{"id", "val", "updated"},
			Inserts: [][]string{
				{strconv.Itoa(i), strconv.Itoa(i), "false"},
				{strconv.Itoa(j), strconv.Itoa(j), "false"}},
		}

		table.insertQ(q)
	}

	// delete half of the table
	q := query.Query{
		Type:      query.Delete,
		TableName: tn,
		Conditions: []query.Condition{
			{Operand1: "id", Operand1IsField: true, Operator: query.Lt, Operand2: strconv.Itoa(N / 3), Operand2IsField: false},
		},
	}
	// double delete
	table.deleteQ(q)
	q.Conditions[0].Operand2 = strconv.Itoa(N / 2)
	table.deleteQ(q)

	// we should have only N/2 records, let's check that out
	q = query.Query{
		Type:      query.Select,
		TableName: tn,
		Fields:    []string{"*"},
	}
	qr := table.selectQ(q)
	expected := N - N/2

	if len(qr.Rows) != expected {
		t.Errorf("Expected %d rows, got %d rows", expected, len(qr.Rows))
	}
}

func TestIncorrectSchema(t *testing.T) {
	tn := "NewTable"
	sch := schema{name: []string{"id", "val", "date"}, colType: []FieldType{INT, TEXT, DATETIME}}
	table := newTable(tn, sch)

	// SELECT
	q := query.Query{Type: query.Select, TableName: tn, Fields: []string{"*", "val_"}}
	qr := table.selectQ(q)
	if qr.Status != "Schema error" {
		t.Errorf("Expected schema error, got %s", qr.Status)
	}
	if qr.Err.Error() != "schema violation: field val_ not defined" {
		t.Errorf("Unexpected error message: %s", qr.Err.Error())
	}

	// INSERT
	q = query.Query{Type: query.Insert, TableName: tn, Fields: []string{"id", "val_"}}

	qr = table.insertQ(q)
	if qr.Status != "Schema error" {
		t.Errorf("Expected schema error, got %s", qr.Status)
	}
	if qr.Err.Error() != "schema violation: field val_ not defined" {
		t.Errorf("Unexpected error message: %s", qr.Err.Error())
	}

	// CREATE INDEX
	// INSERT
	q = query.Query{Type: query.CreateIndex, TableName: tn, Fields: []string{"id_", "val_"}}

	qr = table.createIndexQ(q)
	if qr.Status != "Schema error" {
		t.Errorf("Expected schema error, got %s", qr.Status)
	}
	if qr.Err.Error() != "schema violation: field id_ not defined" {
		t.Errorf("Unexpected error message: %s", qr.Err.Error())
	}
}

func TestEvalConditions(t *testing.T) {
	tn := "NewTable"
	sch := schema{name: []string{"id", "val", "updated"}, colType: []FieldType{INT, TEXT, BOOL}}
	table := newTable(tn, sch)

	qc := []query.Condition{{Operand1: "id", Operand1IsField: true, Operator: 4, Operand2: "50", Operand2IsField: false}}
	rec := record{cells: []string{
		"97",
		"Some value that corresponds to 97",
		"false",
	}}

	res := table.evalConditions(qc, &rec)

	if res {
		t.Errorf("97 < 50")
	}
}

func TestMultithreadedAccess(t *testing.T) {
	const N = 20
	const N2 = N * N
	var insertCnt uint64
	tn := "NewTable"
	sch := schema{name: []string{"id", "val", "updated"}, colType: []FieldType{INT, TEXT, BOOL}}
	table := newTable(tn, sch)

	insertFn := func(id, val string) {
		q := query.Query{
			Type:      query.Insert,
			TableName: tn,
			Fields:    []string{"id", "val", "updated"},
			Inserts:   [][]string{{id, val, "false"}},
		}

		qr := table.insertQ(q)
		if qr.Err == nil && qr.Status == "OK" {
			atomic.AddUint64(&insertCnt, 1)
		}
	}

	update1Fn := func(id_gt, id_lt int) {
		q := query.Query{
			Type:      query.Update,
			TableName: tn,
			Updates:   map[string]string{"updated": "true"},
			Conditions: []query.Condition{
				{Operand1: "id", Operand1IsField: true, Operator: query.Gt, Operand2: strconv.Itoa(id_gt), Operand2IsField: false},
				{Operand1: "id", Operand1IsField: true, Operator: query.Lt, Operand2: strconv.Itoa(id_lt), Operand2IsField: false},
			},
		}
		table.updateQ(q)
	}
	update2Fn := func(id_ge, id_le int) {
		q := query.Query{
			Type:      query.Update,
			TableName: tn,
			Updates:   map[string]string{"updated": "true"},
			Conditions: []query.Condition{
				{Operand1: "id", Operand1IsField: true, Operator: query.Gte, Operand2: strconv.Itoa(id_ge), Operand2IsField: false},
				{Operand1: "id", Operand1IsField: true, Operator: query.Lte, Operand2: strconv.Itoa(id_le), Operand2IsField: false},
			},
		}
		table.updateQ(q)
	}

	deleteFn := func(id_lt int) {
		q := query.Query{
			Type:      query.Delete,
			TableName: tn,
			Conditions: []query.Condition{
				{Operand1: "id", Operand1IsField: true, Operator: query.Lt, Operand2: strconv.Itoa(id_lt), Operand2IsField: false},
			},
		}
		table.deleteQ(q)
	}

	selectUpdatedFn := func() {
		q := query.Query{
			Type:      query.Select,
			TableName: tn,
			Fields:    []string{"id", "val"},
			Conditions: []query.Condition{
				{Operand1: "updated", Operand1IsField: true, Operator: query.Ne, Operand2: "true", Operand2IsField: false},
			},
		}
		table.selectQ(q)

		q = query.Query{
			Type:      query.Select,
			TableName: tn,
			Fields:    []string{"*"},
			Conditions: []query.Condition{
				{Operand1: "updated", Operand1IsField: true, Operator: query.Eq, Operand2: "false", Operand2IsField: false},
			},
		}
		table.selectQ(q)
	}
	// we're going to insert concurrently N * N records
	// we will be updating some of them in the meantime
	// we will be deleting some of them as well
	// then we will check the contraints
	wg := &sync.WaitGroup{}
	wg_insert := &sync.WaitGroup{}
	insertDone := make(chan struct{})

	// schedule SELECT workers
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				selectUpdatedFn()
				runtime.Gosched()
				select {
				case <-insertDone:
					return
				default:
				}
			}
		}()
	}

	// schedule INSERT workers
	for i := 0; i < N; i++ {
		from := i * N
		to := (i + 1) * N
		wg_insert.Add(1)

		go func(from, to int) {
			defer wg_insert.Done()
			for i := from; i < to; i++ {
				insertFn(strconv.Itoa(i), fmt.Sprintf("Some value that corresponds to %d", i))
				runtime.Gosched()
			}
		}(from, to)
	}

	// schedule UPDATE workers
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			update1Fn(0, N2/10)
			runtime.Gosched()
			select {
			case <-insertDone:
				return
			default:
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			update2Fn(N2/12, N2/2)
			runtime.Gosched()
			select {
			case <-insertDone:
				return
			default:
			}
		}
	}()
	// schedule DELETE WORKERS
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
		Loop:
			for {
				deleteFn(N2 / 2)
				runtime.Gosched()
				select {
				case <-insertDone:
					break Loop
				default:
				}
			}
			deleteFn(N2 / 2)
		}()
	}

	// Wait for inserts completion
	wg_insert.Wait()
	close(insertDone)

	if insertCnt != N2 {
		t.Errorf("Expected %d records inserted, got %d", N2, insertCnt)
	}

	wg.Wait()

	// we should have only N/2 records, let's check that out
	q := query.Query{
		Type:      query.Select,
		TableName: tn,
		Fields:    []string{"*"},
	}
	qr := table.selectQ(q)
	expected := N2 - N2/2

	if len(qr.Rows) != expected {
		t.Errorf("Expected %d rows, got %d rows", expected, len(qr.Rows))

		present := make([]bool, N2)
		for i := range qr.Rows {
			id, _ := strconv.Atoi(qr.Rows[i].Fields["id"])
			if !present[id] {
				present[id] = true
			} else {
				t.Logf("Record id=%d already visible", id)
			}
			if id < N2/2 {
				t.Logf("Record id=%d val=%s updated=%s shouldn't be here\n", id, qr.Rows[i].Fields["val"], qr.Rows[i].Fields["updated"])
			}
		}
		for i := N2 / 2; i < N2; i++ {
			if !present[i] {
				t.Logf("Missing id=%d\n", i)
			}
		}
	}
}

func createSimpleTestTable(ids []int) *table {
	tn := "NewTable"
	sch := schema{name: []string{"id"}, colType: []FieldType{INT}}
	table := newTable(tn, sch)

	for _, id := range ids {
		q := query.Query{
			Type:      query.Insert,
			TableName: tn,
			Fields:    []string{"id"},
			Inserts:   [][]string{{strconv.Itoa(id)}},
		}

		table.insertQ(q)
	}

	return table
}

func getAllSimpleTestTable(table *table) []int {
	q := query.Query{
		Type:      query.Select,
		TableName: table.name,
		Fields:    []string{"*"},
	}
	qr := table.selectQ(q)
	var ret []int
	for _, row := range qr.Rows {
		id := row.Fields["id"]
		idd, _ := strconv.Atoi(id)
		ret = append(ret, idd)
	}
	return ret
}

func deleteLessThanTestTable(table *table, lt int) {
	q := query.Query{
		Type:      query.Delete,
		TableName: table.name,
		Conditions: []query.Condition{
			{Operand1: "id", Operand1IsField: true, Operator: query.Lt, Operand2: strconv.Itoa(lt), Operand2IsField: false},
		},
	}
	table.deleteQ(q)
}

func TestSpecificDeletePatern(t *testing.T) {
	table := createSimpleTestTable([]int{5, 2, 4, 1})
	deleteLessThanTestTable(table, 3)
	ret := getAllSimpleTestTable(table)

	if len(ret) != 2 {
		t.Errorf("Unexpected size = %d (%v)", len(ret), ret)
	}
}
