module github.com/kapitanov/natandb/pkg/db

go 1.16

replace github.com/kapitanov/natandb/pkg/log => ../log

replace github.com/kapitanov/natandb/pkg/model => ../model

replace github.com/kapitanov/natandb/pkg/storage => ../storage

replace github.com/kapitanov/natandb/pkg/util => ../util

replace github.com/kapitanov/natandb/pkg/writeahead => ../writeahead

require (
	github.com/kapitanov/natandb/pkg/log v0.0.0-00010101000000-000000000000
	github.com/kapitanov/natandb/pkg/model v0.0.0-00010101000000-000000000000
	github.com/kapitanov/natandb/pkg/storage v0.0.0-00010101000000-000000000000
	github.com/kapitanov/natandb/pkg/writeahead v0.0.0-00010101000000-000000000000
)
