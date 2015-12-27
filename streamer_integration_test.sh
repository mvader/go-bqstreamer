#!/bin/sh
set -x
go test ./async/... -v -tags=integration -key key.json -project $1 -dataset test -table test
