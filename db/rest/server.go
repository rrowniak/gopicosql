package rest

import (
	"fmt"
	"gopicosql/db/engine"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/gin-gonic/gin"
)

var (
	WarningLogger *log.Logger
	InfoLogger    *log.Logger
	ErrorLogger   *log.Logger
)

func init() {
	InfoLogger = log.New(os.Stdout, "[GO-PICO-SQL] INFO: ", log.Ldate|log.Ltime|log.Lshortfile)
	WarningLogger = log.New(os.Stdout, "[GO-PICO-SQL] WARNING: ", log.Ldate|log.Ltime|log.Lshortfile)
	ErrorLogger = log.New(os.Stdout, "[GO-PICO-SQL] ERROR: ", log.Ldate|log.Ltime|log.Lshortfile)
}

func NewServer(cfg *engine.Cfg) (*Server, error) {
	return &Server{cfg: cfg}, nil
}

type Server struct {
	cfg     *engine.Cfg
	db      *engine.DbEngine
	status  string
	lastLog string
}

func (s *Server) Run() {
	// configure Gin server
	router := gin.Default()
	router.POST("/query", s.execSqlQuery)
	router.GET("/status", s.queryStatus)
	router.GET("/version", s.queryVersion)

	// set up the db engine
	var err error
	s.db, err = engine.NewDbEngine(s.cfg)
	if err != nil {
		s.status = "error"
		s.lastLog = err.Error()
	}
	s.db.Start()
	defer s.db.Stop()

	s.status = "running"
	router.Run(fmt.Sprintf("%s:%d", s.cfg.ServHost, s.cfg.ServPort))
}

type queryRow struct {
	Fields map[string]string `json:"fields"`
}

type queryResponse struct {
	Result string     `json:"result"`
	Error  string     `json:"error"`
	Rows   []queryRow `json:"rows"`
}

func (s *Server) execSqlQuery(c *gin.Context) {
	sql := c.PostForm("sql")

	InfoLogger.Printf("Received SQL request: '%s'", sql)

	respChan := make(chan engine.QueryResult)
	r := engine.QueryRequest{Sql: sql, Resp: respChan}
	s.db.ProcessQuery(r)

	resp := queryResponse{}

	status := http.StatusOK

	select {
	case qr := <-respChan:
		resp.Result = qr.Status
		if qr.Err != nil {
			resp.Error = qr.Err.Error()
			status = http.StatusBadRequest
		}
		resp.Rows = make([]queryRow, len(qr.Rows))
		for _, r := range qr.Rows {
			resp.Rows = append(resp.Rows, queryRow{Fields: r.Fields})
		}
	case <-time.After(time.Duration(s.cfg.QueryTimeoutSecs) * time.Second):
		resp.Result = "query timeout"
		status = http.StatusServiceUnavailable
	}

	c.IndentedJSON(status, resp)
}

func (s *Server) queryStatus(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": s.status, "last_log": s.lastLog})
}

func (s *Server) queryVersion(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"db_version": "1.0", "API_version": "1.0"})
}
