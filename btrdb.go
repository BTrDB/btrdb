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

type M map[string]string

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

func (b *BTrDB) Create(ctx context.Context, uu uuid.UUID, collection string, tags map[string]string) error {
	taglist := []*pb.Tag{}
	for k, v := range tags {
		taglist = append(taglist, &pb.Tag{Key: k, Value: v})
	}
	rv, err := b.g.Create(ctx, &pb.CreateParams{
		Uuid:       uu,
		Collection: collection,
		Tags:       taglist,
	})
	if err != nil {
		return err
	}
	if rv.GetStat() != nil {
		return &CodedError{rv.GetStat()}
	}
	return nil
}

//ListAllCollections is shorthand for ListCollection with an empty string for
//prefix and from, and a million for maxnumber. It will return an error if there
//are more than a million collections on the cluster, so it may be preferable
//to use ListCollections() if that is anticipated.
func (b *BTrDB) ListAllCollections(ctx context.Context) ([]string, error) {
	rv, err := b.g.ListCollections(ctx, &pb.ListCollectionsParams{
		Prefix:    "",
		StartWith: "",
		Number:    2000000,
	})
	if err != nil {
		return nil, err
	}
	if rv.GetStat() != nil {
		return nil, &CodedError{rv.GetStat()}
	}
	if len(rv.Collections) >= 2000000 {
		return nil, fmt.Errorf("You cannot use ListAllCollections if the number of collections exceeds 1 million. Please use ListCollections")
	}
	return rv.Collections, nil
}

//ListCollections will allow paginated iteration of all the collections on the cluster. If prefix and from are empty strings,
//it will return all collections, up to maxnumber. Note that collections have an ordering that is stable but not entirely
//alphabetical. For this reason, the "from" parameter should usually be derived from the last element of the returned
//list of collections from a previous invocation of ListCollections. It is generally not possible to guess the ordering
//in advance. If maxnumber is 0 it defaults to 10000.
func (b *BTrDB) ListCollections(ctx context.Context, prefix string, from string, maxnumber uint64) ([]string, error) {
	if maxnumber > 10000 {
		return nil, fmt.Errorf("Maxnumber must be <10k")
	}
	if maxnumber == 0 {
		maxnumber = 10000
	}
	rv, err := b.g.ListCollections(ctx, &pb.ListCollectionsParams{
		Prefix:    prefix,
		StartWith: from,
		Number:    maxnumber,
	})
	if err != nil {
		return nil, err
	}
	if rv.GetStat() != nil {
		return nil, &CodedError{rv.GetStat()}
	}
	return rv.Collections, nil
}

type Stream struct {
	UUID []byte
	Tags map[string]string
}

//ListAllStreams will return all the streams in the named collection
func (b *BTrDB) ListAllStreams(ctx context.Context, collection string) ([]*Stream, error) {
	rv, err := b.g.ListStreams(ctx, &pb.ListStreamsParams{
		Collection: collection,
		Partial:    true,
	})
	if err != nil {
		return nil, err
	}
	if rv.GetStat() != nil {
		return nil, &CodedError{rv.GetStat()}
	}
	sl := rv.StreamListings
	nrv := make([]*Stream, 0, 10)
	for _, s := range sl {
		nrv = append(nrv, &Stream{UUID: s.Uuid})
	}
	return nrv, nil
}

//ListMatchingStreams will return all streams in a collection that match the specified tags. If no tags are specified this
//method is equivalent to ListAllStreams()
func (b *BTrDB) ListMatchingStreams(ctx context.Context, collection string, tags map[string]string) ([]*Stream, error) {
	params := &pb.ListStreamsParams{
		Collection: collection,
		Partial:    true,
	}
	for k, v := range tags {
		params.Tags = append(params.Tags, &pb.Tag{Key: k, Value: v})
	}
	rv, err := b.g.ListStreams(ctx, params)
	if err != nil {
		return nil, err
	}
	if rv.GetStat() != nil {
		return nil, &CodedError{rv.GetStat()}
	}
	sl := rv.StreamListings
	nrv := make([]*Stream, 0, 10)
	for _, s := range sl {
		nrv = append(nrv, &Stream{UUID: s.Uuid})
	}
	return nrv, nil
}

//LookupStream takes a collection and a full set of tags and returns the resulting stream, or nil if the
//stream was not found. It is an error to omit any tags, even if the stream is uniquely identified by only
//a subset of the tags. To locate a stream with a unique subset of tags, use ListMatchingStreams.
func (b *BTrDB) LookupStream(ctx context.Context, collection string, tags map[string]string) (*Stream, error) {
	params := &pb.ListStreamsParams{
		Collection: collection,
		Partial:    false,
	}
	for k, v := range tags {
		params.Tags = append(params.Tags, &pb.Tag{Key: k, Value: v})
	}
	rv, err := b.g.ListStreams(ctx, params)
	if err != nil {
		return nil, err
	}
	if rv.GetStat() != nil {
		return nil, &CodedError{rv.GetStat()}
	}
	if len(rv.StreamListings) == 0 {
		return nil, nil
	}
	s := rv.StreamListings[0]
	srv := &Stream{UUID: s.Uuid}
	return srv, nil
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

func (b *BTrDB) DeleteRange(ctx context.Context, uu uuid.UUID, start int64, end int64) error {
	rv, err := b.g.Delete(ctx, &pb.DeleteParams{
		Uuid:  uu,
		Start: start,
		End:   end,
	})
	if err != nil {
		return err
	}
	if rv.Stat != nil {
		return &CodedError{rv.Stat}
	}
	return nil
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

func (b *BTrDB) Nearest(ctx context.Context, uu uuid.UUID, time int64, version uint64, backward bool) (*pb.RawPoint, uint64, error) {
	rv, err := b.g.Nearest(ctx, &pb.NearestParams{
		Uuid:         uu,
		Time:         time,
		VersionMajor: version,
		Backward:     backward,
	})
	if err != nil {
		return nil, 0, err
	}
	if rv.Stat != nil {
		return nil, 0, &CodedError{rv.Stat}
	}
	return rv.Value, rv.VersionMajor, nil
}

func (b *BTrDB) Changes(ctx context.Context, uu uuid.UUID, fromVersion uint64, toVersion uint64, resolution uint8) (chan *pb.ChangedRange, chan error) {
	rv, err := b.g.Changes(ctx, &pb.ChangesParams{
		Uuid:       uu,
		FromMajor:  fromVersion,
		ToMajor:    toVersion,
		Resolution: uint32(resolution),
	})
	rvc := make(chan *pb.ChangedRange, 100)
	rve := make(chan error, 1)
	if err != nil {
		close(rvc)
		rve <- err
		close(rve)
		return rvc, rve
	}
	go func() {
		for {
			cr, err := rv.Recv()
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
			if cr.Stat != nil {
				close(rvc)
				rve <- &CodedError{cr.Stat}
				close(rve)
				return
			}
			for _, x := range cr.Ranges {
				rvc <- x
			}
		}
	}()
	return rvc, rve
}
