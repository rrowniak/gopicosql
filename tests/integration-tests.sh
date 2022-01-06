#!/bin/sh

[ -z "${HOST}" ] && HOST=localhost
[ -z "${PORT}" ] && PORT=8080

echo "Connecting to the db server at: $HOST:$PORT"

fail() {
    echo "$1"
    exit 1
}

command -v curl > /dev/null || fail "Curl not installed"

sql() {
    curl -sS -X POST $HOST:$PORT/query -d "sql=$1"
}

test_case1() {
    OUT=$(curl -sS -X POST $HOST:$PORT/status)
    if [ $? -ne 0 ]; then
        echo "$OUT"
        return 1
    fi
    return 0
}

test_case2() {
    SQL="CREATE TABLE test (id INT, created DATETIME, data TEXT, valid BOOL)"
    OUT=$(sql "$SQL")
    if [ $? -ne 0 ]; then
        echo "$OUT"
        return 1
    fi

    if ! echo "$OUT" | grep -q "OK"; then
        echo "$OUT"
        echo "Expected succesfull call"
        return 1
    fi
    
    return 0
}

test_case3() {
    SQL="INSERT INTO test (id, created, data, valid) VALUES ('0', '2022-01-06', 'testing database', 'true')"
    OUT=$(sql "$SQL")
    if [ $? -ne 0 ]; then
        echo "$OUT"
        return 1
    fi

    if ! echo "$OUT" | grep -q "OK"; then
        echo "$OUT"
        echo "Expected succesfull call"
        return 1
    fi
    
    return 0
}

TEST_CASE_NUM=3

# wait for the server
for i in $(seq 1 5); do
    if curl -s -X POST $HOST:$PORT/status; then
        break
    fi
    sleep 1
done

for i in $(seq 1 $TEST_CASE_NUM); do
    echo "========================================="
    echo "Executing test_case$i..."
    
    if ! test_case$i; then
        echo "test_case$i failed. Aborting execution"
        exit 1
    else
        echo "test_case$i OK"
    fi

done