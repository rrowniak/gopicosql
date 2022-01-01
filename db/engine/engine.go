package engine

import (
	"fmt"
	"sync"
	"time"

	"github.com/rrowniak/sqlparser"
	"github.com/rrowniak/sqlparser/query"
)

func NewDbEngine(cfg *Cfg) (*DbEngine, error) {
	return &DbEngine{cfg: cfg, lockTables: &sync.RWMutex{}, tables: make(map[string]*table)}, nil
}

type QueryRequest struct {
	Sql  string
	Resp chan QueryResult
}

type QueryResult struct {
	Err    error
	Status string
	Rows   []Row
}

type Row struct {
	Fields map[string]string
}

type DbEngine struct {
	cfg            *Cfg
	lockTables     *sync.RWMutex
	quit           chan struct{}
	requests       chan QueryRequest
	reqWorkersPool chan struct{}

	tables map[string]*table
}

func (db *DbEngine) Start() {
	db.quit = make(chan struct{})
	db.requests = make(chan QueryRequest, db.cfg.MaxDbRequests)
	db.reqWorkersPool = make(chan struct{}, db.cfg.MaxDbRequests*2)
	go db.main()
}

func (db *DbEngine) Stop() {
	close(db.requests)
	db.quit <- struct{}{}
	close(db.quit)
}

func (db *DbEngine) ProcessQuery(req QueryRequest) {
	db.requests <- req
}

func (db *DbEngine) execQuery(req QueryRequest) {
	result := QueryResult{Status: "Unexpected failure"}
	defer func() {
		<-db.reqWorkersPool
		req.Resp <- result
	}()

	actual, err := sqlparser.Parse(req.Sql)
	if err != nil {
		result.Status = "Syntax error"
		result.Err = err
		return
	}

	result.Status = "Logic error"

	if actual.Type == query.Create {
		db.lockTables.RLock()
		if _, ok := db.tables[actual.TableName]; ok {
			result.Err = fmt.Errorf("table %s already exists", actual.TableName)
			db.lockTables.RUnlock()
			return
		}
		db.lockTables.RUnlock()

		var sch schema

		for _, f := range actual.Fields {
			t, ok := actual.Updates[f]
			if !ok {
				result.Err = fmt.Errorf("field %s type definition missing", f)
				return
			}
			ft := fieldTypeFromString(t)
			if ft == UNKNOWN_FIELD_TYPE {
				result.Err = fmt.Errorf("field %s type is not supported", t)
				return
			}
			sch.name = append(sch.name, f)
			sch.colType = append(sch.colType, ft)
		}
		db.lockTables.Lock()
		db.tables[actual.TableName] = newTable(actual.TableName, sch)
		db.lockTables.Unlock()

		result.Status = "OK"
		return
	}

	db.lockTables.RLock()
	table, ok := db.tables[actual.TableName]
	db.lockTables.RUnlock()

	if !ok {
		result.Err = fmt.Errorf("table %s does not exist", actual.TableName)
		return
	}

	switch actual.Type {
	case query.Select:
		result = table.selectQ(actual)
	case query.Update:
		result = table.updateQ(actual)
	case query.Insert:
		result = table.insertQ(actual)
	case query.Delete:
		result = table.deleteQ(actual)
	case query.Drop:
		table.dropQ()
		result.Status = "OK"
	case query.CreateIndex:
		result = table.createIndexQ(actual)
	}
}

func (db *DbEngine) processRequests() {
	for {
		select {
		case req := <-db.requests:
			db.reqWorkersPool <- struct{}{}
			go db.execQuery(req)
		default:
			return
		}
	}
}

func (db *DbEngine) main() {
	compactTimer := time.NewTimer(time.Duration(db.cfg.CompactEverySecs) * time.Second)
	for {
		select {
		case <-db.quit:
			return
		case <-compactTimer.C:
			// launch compact operation
		default:
			// do other stuff
			db.processRequests()
		}
	}
}
