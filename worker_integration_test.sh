#!/bin/sh
set -x
go test ./async/worker/... -parallel 1 -cpu 1 -v -tags=integration -key key.json -project $1 -dataset test -table test
