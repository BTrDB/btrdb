package btrdb

import (
	"fmt"
	"os"
	"strings"

	pb "github.com/BTrDB/btrdb/v5/v5api"
)

//M is an alias to neaten code specifying tags:
// btrdb.LookupStream(ctx, "mycollection", btrdb.M{"tagkey":"tagval"})
type M map[string]string

//CodedError is an error that contains a numeric code. Most errors returned by
//this package are actually *CodedError objects. Use ToCodedError()
type CodedError struct {
	*pb.Status
}

//Error() implements the error interface
func (ce *CodedError) Error() string {
	return fmt.Sprintf("[%d] %s", ce.Code, ce.Msg)
}

//ToCodedError can be used to convert any error into a CodedError. If the
//error object is actually not coded, it will receive code 501.
func ToCodedError(e error) *CodedError {
	if e == nil {
		return nil
	} else if ce, ok := e.(*CodedError); ok {
		return ce
	} else {
		s := pb.Status{Code: 501, Msg: e.Error()}
		return &CodedError{&s}
	}
}

//LatestVersion can be passed to any functions taking a version to use the
//latest version of that stream
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
