package engine

import (
	"fmt"
	"strconv"
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
			t.Errorf("Expected %d row, got %d rows", 100-i, len(qr.Rows))
		}
	}
}
