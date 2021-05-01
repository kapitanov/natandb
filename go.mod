module github.com/kapitanov/natandb

go 1.16

replace github.com/kapitanov/natandb/pkg/log => ./pkg/log

replace github.com/kapitanov/natandb/pkg/proto => ./pkg/proto

replace github.com/kapitanov/natandb/pkg/db => ./pkg/db

replace github.com/kapitanov/natandb/pkg/model => ./pkg/model

replace github.com/kapitanov/natandb/pkg/storage => ./pkg/storage

replace github.com/kapitanov/natandb/pkg/util => ./pkg/util

replace github.com/kapitanov/natandb/pkg/writeahead => ./pkg/writeahead

replace github.com/kapitanov/natandb/cmd => ./cmd

replace github.com/kapitanov/natandb/cmd/diag => ./cmd/diag

replace github.com/kapitanov/natandb/cmd/test => ./cmd/test

require (
	github.com/kapitanov/natandb/cmd v0.0.0-00010101000000-000000000000
	github.com/mattn/go-colorable v0.1.2 // indirect
)
