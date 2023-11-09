#!/bin/bash

echo "$0, $1"

go build -buildmode=plugin ../mrapps/${1}.go

go run mrworker.go ${1}.so
