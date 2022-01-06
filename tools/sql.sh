#!/bin/sh

HOST=localhost
PORT=8080

fail() {
    echo $1
    exit 1
}

command -v curl > /dev/null || fail "Curl not installed"

curl -X POST $HOST:$PORT/query -d "sql=$1"
echo