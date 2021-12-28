#!/bin/bash

SQL_TOOL=../tools/sql.sh

function sql() {
    query="$1"
    echo "Executing '$query':"
    $SQL_TOOL "$query"
}

sql "CREATE TABLE test_table (id INT, created DATETIME, url TEXT, valid BOOL)"
sql "SELECT * FROM test_table"
sql "INSERT INTO test_table (id, created, url, valid) VALUES ('0', '2021-12-28 15:51', 'rrowniak.com', 'true')"
sql "INSERT INTO test_table (id, created, url, valid) VALUES ('1', '2021-12-28 15:51', 'google.com', 'true'), ('2', '2021-12-28 15:51', 'yahoo.com', 'false')"
sql "SELECT * FROM test_table"