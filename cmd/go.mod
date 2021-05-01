module github.com/kapitanov/natandb/cmd

go 1.16

replace github.com/kapitanov/natandb/pkg/log => ../pkg/log

replace github.com/kapitanov/natandb/pkg/proto => ../pkg/proto

replace github.com/kapitanov/natandb/pkg/db => ../pkg/db

replace github.com/kapitanov/natandb/pkg/model => ../pkg/model

replace github.com/kapitanov/natandb/pkg/storage => ../pkg/storage

replace github.com/kapitanov/natandb/pkg/util => ../pkg/util

replace github.com/kapitanov/natandb/pkg/writeahead => ../pkg/writeahead

replace github.com/kapitanov/natandb/cmd/diag => ./diag

replace github.com/kapitanov/natandb/cmd/test => ./test

require (
	github.com/gosuri/uitable v0.0.4
	github.com/kapitanov/natandb/cmd/diag v0.0.0-00010101000000-000000000000
	github.com/kapitanov/natandb/cmd/test v0.0.0-00010101000000-000000000000
	github.com/kapitanov/natandb/pkg/db v0.0.0-00010101000000-000000000000
	github.com/kapitanov/natandb/pkg/log v0.0.0-00010101000000-000000000000
	github.com/kapitanov/natandb/pkg/proto v0.0.0-00010101000000-000000000000
	github.com/kapitanov/natandb/pkg/storage v0.0.0-00010101000000-000000000000
	github.com/kapitanov/natandb/pkg/util v0.0.0-00010101000000-000000000000
	github.com/kapitanov/natandb/pkg/writeahead v0.0.0-00010101000000-000000000000
	github.com/spf13/cobra v1.1.3
)
