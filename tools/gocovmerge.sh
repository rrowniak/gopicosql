#!/bin/env bash


SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
BUILD_DIR=$SCRIPT_DIR/../build

cd "$SCRIPT_DIR" || exit 1

function download_gocovmerge() {
    wget https://raw.githubusercontent.com/wadey/gocovmerge/master/gocovmerge.go
    go get golang.org/x/tools/cover
}

[ -d $BUILD_DIR ] || mkdir -p $BUILD_DIR
[ -f $SCRIPT_DIR/gocovmerge.go ] || download_gocovmerge

PROFILES=$(ls $BUILD_DIR/coverage-TEST-*.out)

go run gocovmerge.go $PROFILES > $BUILD_DIR/coverage-TEST-MERGED.out