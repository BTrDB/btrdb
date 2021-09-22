package v5api

//go:generate protoc -I /usr/local/include -I ./include -I . --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative btrdb.proto
//go:generate protoc -I /usr/local/include -I ./include -I . --grpc-gateway_out=logtostderr=true:. --grpc-gateway_opt=paths=source_relative btrdb.proto
// //go:generate protoc -I/usr/local/include -I ./include -I. --openapiv2_out=logtostderr=true:. btrdb.proto
