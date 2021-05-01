module github.com/kapitanov/natandb/pkg/writeahead

go 1.16

replace github.com/kapitanov/natandb/pkg/storage => ../storage

replace github.com/kapitanov/natandb/pkg/writeahead => ../writeahead

replace github.com/kapitanov/natandb/pkg/util => ../util

replace github.com/kapitanov/natandb/pkg/log => ../log

require (
	github.com/kapitanov/natandb/pkg/log v0.0.0-00010101000000-000000000000
	github.com/kapitanov/natandb/pkg/storage v0.0.0-00010101000000-000000000000
	github.com/kapitanov/natandb/pkg/util v0.0.0-00010101000000-000000000000
)
