package rest

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"gopicosql/db/engine"

	"github.com/gin-gonic/gin"
)

func mockGin(method, query, body_k, body_v string) (c *gin.Context, w *httptest.ResponseRecorder) {
	w = httptest.NewRecorder()
	c, _ = gin.CreateTestContext(w)

	form := url.Values{}
	if len(body_k) != 0 {
		form.Add(body_k, body_v)
	}

	req, _ := http.NewRequest(method, query, strings.NewReader(form.Encode()))
	if method == http.MethodPost {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded; param=value")
	}
	// q := req.URL.Query()
	// req.URL.RawQuery = q.Encode()
	c.Request = req

	return
}

type ftExecQuery struct {
	name     string
	query    string
	respCode int
}

func TestExecQueryHandlerSelectQueries(t *testing.T) {
	tcs := []ftExecQuery{
		{"empty query", "", 400},
		{"invalid query 1", "sql", 400},
		{"invalid query 2", "select *", 400},
		{"valid query select", "select * from db", 400},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			s, _ := NewServer(engine.NewConfigDefault())
			s.setUpDbEng()
			c, w := mockGin(http.MethodPost, "/query", "sql", tc.query)

			s.execSqlQuery(c)

			if w.Code != tc.respCode {
				b, _ := ioutil.ReadAll(w.Body)
				t.Errorf("Expected code %d, got %d: %s", tc.respCode, w.Code, string(b))
			}
		})
	}
}
