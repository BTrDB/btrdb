package btrdb

import (
	"fmt"
	"os"
	"strings"

	pb "gopkg.in/btrdb.v4/grpcinterface"
)

type M map[string]string

type CodedError struct {
	*pb.Status
}

func (ce *CodedError) Error() string {
	return fmt.Sprintf("[%d] %s", ce.Code, ce.Msg)
}

func ToCodedError(e error) *CodedError {
	ce, ok := e.(*CodedError)
	if ok {
		return ce
	}
	s := pb.Status{Code: 501, Msg: e.Error()}
	return &CodedError{&s}
}

const LatestVersion = 0

//EndpointsFromEnv reads the environment variable BTRDB_ENDPOINTS of the format
// server:port,server:port,server:port
//and returns it as a string slice. This function is typically used as
// btrdb.Connect(btrdb.EndpointsFromEnv()...)
func EndpointsFromEnv() []string {
	endpoints := os.Getenv("BTRDB_ENDPOINTS")
	if endpoints == "" {
		return nil
	}
	spl := strings.Split(endpoints, ",")
	return spl
}
