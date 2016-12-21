package btrdb

//go:generate protoc grpcinterface/btrdb.proto --go_out=plugins=grpc:.
import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/pborman/uuid"
	"google.golang.org/grpc"
	pb "gopkg.in/btrdb.v4/grpcinterface"
)

type BTrDB struct {
	g pb.BTrDBClient
}

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
	} else {
		s := pb.Status{Code: 501, Msg: e.Error()}
		return &CodedError{&s}
	}
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

//Connect takes a list of endpoints and returns a BTrDB handle
//BUG(mpa) only the first endpoint is used
//BUG(mpa) this should do an info call and then store
//alternate endpoints in the client (also to verify its ok)
func Connect(endpoints ...string) (*BTrDB, error) {
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("No endpoints provided")
	}
	conn, err := grpc.Dial(endpoints[0], grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	client := pb.NewBTrDBClient(conn)
	rv := &BTrDB{g: client}
	return rv, nil
}

func (b *BTrDB) GetGRPC() pb.BTrDBClient {
	return b.g
}

func (b *BTrDB) Insert(ctx context.Context, uu uuid.UUID, values []*pb.RawPoint) error {
	rv, err := b.g.Insert(ctx, &pb.InsertParams{
		Uuid:   uu,
		Sync:   false,
		Values: values,
	})
	if err != nil {
		return err
	}
	if rv.GetStat() != nil {
		return &CodedError{rv.GetStat()}
	}
	return nil
}

//RawValues reads raw values from BTrDB. The contract is:
//You can defer reading the error channel until after the value
//channel is closed, but you cannot assume there wasn't an error (even if
//you get values) unless you read the error channel.
func (b *BTrDB) RawValues(ctx context.Context, uu uuid.UUID, start int64, end int64, version uint64) (chan *pb.RawPoint, chan error) {
	rv, err := b.g.RawValues(ctx, &pb.RawValuesParams{
		Uuid:         uu,
		Start:        start,
		End:          end,
		VersionMajor: version,
	})
	rvc := make(chan *pb.RawPoint, 100)
	rve := make(chan error, 1)
	if err != nil {
		close(rvc)
		rve <- err
		close(rve)
		return rvc, rve
	}
	go func() {
		for {
			rawv, err := rv.Recv()
			if err == io.EOF {
				close(rvc)
				close(rve)
				return
			}
			if err != nil {
				close(rvc)
				rve <- err
				close(rve)
				return
			}
			if rawv.Stat != nil {
				close(rvc)
				rve <- &CodedError{rawv.Stat}
				close(rve)
				return
			}
			for _, x := range rawv.Values {
				rvc <- x
			}
		}
	}()
	return rvc, rve
}

func (b *BTrDB) AlignedWindows(ctx context.Context, uu uuid.UUID, start int64, end int64, pointwidth uint8, version uint64) (chan *pb.StatPoint, chan error) {
	rv, err := b.g.AlignedWindows(ctx, &pb.AlignedWindowsParams{
		Uuid:         uu,
		Start:        start,
		End:          end,
		PointWidth:   uint32(pointwidth),
		VersionMajor: version,
	})
	rvc := make(chan *pb.StatPoint, 100)
	rve := make(chan error, 1)
	if err != nil {
		close(rvc)
		rve <- err
		close(rve)
		return rvc, rve
	}
	go func() {
		for {
			rawv, err := rv.Recv()
			if err == io.EOF {
				close(rvc)
				close(rve)
				return
			}
			if err != nil {
				close(rvc)
				rve <- err
				close(rve)
				return
			}
			if rawv.Stat != nil {
				close(rvc)
				rve <- &CodedError{rawv.Stat}
				close(rve)
				return
			}
			for _, x := range rawv.Values {
				rvc <- x
			}
		}
	}()
	return rvc, rve
}
func (b *BTrDB) Windows(ctx context.Context, uu uuid.UUID, start int64, end int64, width uint64, depth uint8, version uint64) (chan *pb.StatPoint, chan error) {
	rv, err := b.g.Windows(ctx, &pb.WindowsParams{
		Uuid:         uu,
		Start:        start,
		End:          end,
		Width:        width,
		Depth:        uint32(depth),
		VersionMajor: version,
	})
	rvc := make(chan *pb.StatPoint, 100)
	rve := make(chan error, 1)
	if err != nil {
		close(rvc)
		rve <- err
		close(rve)
		return rvc, rve
	}
	go func() {
		for {
			rawv, err := rv.Recv()
			if err == io.EOF {
				close(rvc)
				close(rve)
				return
			}
			if err != nil {
				close(rvc)
				rve <- err
				close(rve)
				return
			}
			if rawv.Stat != nil {
				close(rvc)
				rve <- &CodedError{rawv.Stat}
				close(rve)
				return
			}
			for _, x := range rawv.Values {
				rvc <- x
			}
		}
	}()
	return rvc, rve
}

func (b *BTrDB) Info(ctx context.Context) (*pb.Mash, error) {
	rv, err := b.g.Info(ctx, &pb.InfoParams{})
	if err != nil {
		return nil, err
	}
	if rv.Stat != nil {
		return nil, &CodedError{rv.Stat}
	}
	return rv.Mash, nil
}
