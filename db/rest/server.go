package rest

import (
	"fmt"
	"gopicosql/db/engine"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

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

	s.status = "running"
	router.Run(fmt.Sprintf("%s:%d", s.cfg.ServHost, s.cfg.ServPort))
}

type queryRow struct {
	Fields map[string]string `json:"fields"`
}

type queryResponse struct {
	Result string     `json:"result"`
	Rows   []queryRow `json:"rows"`
}

func (s *Server) execSqlQuery(c *gin.Context) {
	sql := c.PostForm("sql")

	respChan := make(chan engine.QueryResult)
	r := engine.QueryRequest{Sql: sql, Resp: respChan}
	s.db.ProcessQuery(r)

	resp := queryResponse{}

	select {
	case qr := <-respChan:
		resp.Result = "OK"
		resp.Rows = make([]queryRow, len(qr.Rows))
		for _, r := range qr.Rows {
			resp.Rows = append(resp.Rows, queryRow{Fields: r.Fields})
		}
	case <-time.After(time.Duration(s.cfg.QueryTimeoutSecs) * time.Second):
		resp.Result = "query timeout"
	}

	c.IndentedJSON(http.StatusOK, resp)
}

func (s *Server) queryStatus(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": s.status, "last_log": s.lastLog})
}

func (s *Server) queryVersion(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"db_version": "1.0", "API_version": "1.0"})
}
