package v5api

//go:generate protoc -I . -I /usr/local/include -I ./include --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative btrdb.proto
