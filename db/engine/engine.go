package engine

import (
	"time"
	"github.com/rrowniak/sqlparser"
)

func NewDbEngine(cfg *Cfg) (*DbEngine, error) {
	return &DbEngine{cfg: cfg}, nil
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
	cfg      *Cfg
	quit     chan struct{}
	requests chan QueryRequest
	reqWorkersPool chan struct{}
}

func (db *DbEngine) Start() {
	db.quit = make(chan struct{})
	db.requests = make(chan QueryRequest, db.cfg.MaxDbRequests)
	db.reqWorkersPool = make(chan struct{}, db.cfg.MaxDbRequests * 2)
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
	
	actual, err := sqlparser.ParseMany([]string{req.Sql})
	if err != nil {
		result.Status = "Syntax error"
		result.Err = err
		return
	}

	result.Status = actual[0].TableName
	
}

func (db *DbEngine) processRequests() {
	for {
		select {
		case req := <- db.requests:
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
