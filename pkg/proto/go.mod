module github.com/kapitanov/natandb/pkg/proto

go 1.16

replace github.com/kapitanov/natandb/pkg/db => ../db

replace github.com/kapitanov/natandb/pkg/log => ../log

replace github.com/kapitanov/natandb/pkg/model => ../model

replace github.com/kapitanov/natandb/pkg/storage => ../storage

replace github.com/kapitanov/natandb/pkg/util => ../util

replace github.com/kapitanov/natandb/pkg/writeahead => ../writeahead

require (
	github.com/golang/protobuf v1.5.2
	github.com/kapitanov/natandb/pkg/db v0.0.0-00010101000000-000000000000
	github.com/kapitanov/natandb/pkg/log v0.0.0-00010101000000-000000000000
	google.golang.org/grpc v1.37.0
)
