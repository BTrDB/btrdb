package btrdb

//go:generate protoc grpcinterface/btrdb.proto --go_out=plugins=grpc:.
import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/pborman/uuid"
	"google.golang.org/grpc"
	pb "gopkg.in/btrdb.v4/grpcinterface"
)

type BTrDBEndpoint struct {
	g    pb.BTrDBClient
	conn *grpc.ClientConn
}

type RawPoint struct {
	Time  int64
	Value float64
}

//ConnectEndpoint is a low level call that connects to a single BTrDB
//server. It takes multiple arguments, but it is assumed that they are
//all different addresses for the same server, in decreasing order of
//priority
func ConnectEndpoint(ctx context.Context, addresses ...string) (*BTrDBEndpoint, error) {
	if len(addresses) == 0 {
		return nil, fmt.Errorf("No addresses provided")
	}
	for _, a := range addresses {
		dl, ok := ctx.Deadline()
		if !ok {
			return nil, context.DeadlineExceeded
		}
		tmt := dl.Sub(time.Now())
		if tmt > 2*time.Second {
			tmt = 2 * time.Second
		}
		conn, err := grpc.Dial(a, grpc.WithInsecure(), grpc.WithTimeout(tmt), grpc.WithBlock())
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			continue
		}
		client := pb.NewBTrDBClient(conn)
		rv := &BTrDBEndpoint{g: client, conn: conn}
		return rv, nil
	}
	return nil, fmt.Errorf("Endpoint is unreachable on all addresses")
}

func (b *BTrDBEndpoint) GetGRPC() pb.BTrDBClient {
	return b.g
}

func (b *BTrDBEndpoint) Disconnect() error {
	return b.conn.Close()
}

func (b *BTrDBEndpoint) Insert(ctx context.Context, uu uuid.UUID, values []*pb.RawPoint) error {
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

func (b *BTrDBEndpoint) Create(ctx context.Context, uu uuid.UUID, collection string, tags map[string]string) error {
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
func (b *BTrDBEndpoint) ListAllCollections(ctx context.Context) ([]string, error) {
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
func (b *BTrDBEndpoint) ListCollections(ctx context.Context, prefix string, from string, maxnumber uint64) ([]string, error) {
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

//ListAllStreams will return all the streams in the named collection
func (b *BTrDBEndpoint) ListAllStreams(ctx context.Context, collection string) ([]*Stream, error) {
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
		nrv = append(nrv, &Stream{uuid: s.Uuid, collection: collection, hasCollection: true})
	}
	return nrv, nil
}

//ListMatchingStreams will return all streams in a collection that match the specified tags. If no tags are specified this
//method is equivalent to ListAllStreams()
func (b *BTrDBEndpoint) ListMatchingStreams(ctx context.Context, collection string, tags map[string]string) ([]*Stream, error) {
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
		nrv = append(nrv, &Stream{uuid: s.Uuid, collection: collection, hasCollection: true})
	}
	return nrv, nil
}

//LookupStream takes a collection and a full set of tags and returns the resulting stream, or nil if the
//stream was not found. It is an error to omit any tags, even if the stream is uniquely identified by only
//a subset of the tags. To locate a stream with a unique subset of tags, use ListMatchingStreams.
func (b *BTrDBEndpoint) LookupStream(ctx context.Context, collection string, tags map[string]string) (*Stream, error) {
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
	srv := &Stream{uuid: s.Uuid, collection: collection, hasCollection: true}
	return srv, nil
}

//RawValues reads raw values from BTrDB. The contract is:
//You can defer reading the error channel until after the value
//channel is closed, but you cannot assume there wasn't an error (even if
//you get values) unless you read the error channel.
func (b *BTrDBEndpoint) RawValues(ctx context.Context, uu uuid.UUID, start int64, end int64, version uint64) (chan RawPoint, chan uint64, chan error) {
	rv, err := b.g.RawValues(ctx, &pb.RawValuesParams{
		Uuid:         uu,
		Start:        start,
		End:          end,
		VersionMajor: version,
	})
	rvc := make(chan RawPoint, 100)
	rvv := make(chan uint64, 1)
	rve := make(chan error, 1)
	wroteVer := false
	if err != nil {
		close(rvv)
		close(rvc)
		rve <- err
		close(rve)
		return rvc, rvv, rve
	}
	go func() {
		for {
			rawv, err := rv.Recv()
			if err == io.EOF {
				close(rvc)
				close(rvv)
				close(rve)
				return
			}
			if err != nil {
				close(rvc)
				close(rvv)
				rve <- err
				close(rve)
				return
			}
			if rawv.Stat != nil {
				close(rvc)
				close(rvv)
				rve <- &CodedError{rawv.Stat}
				close(rve)
				return
			}
			if !wroteVer {
				rvv <- rawv.VersionMajor
				wroteVer = true
			}
			for _, x := range rawv.Values {
				rvc <- RawPoint{x.Time, x.Value}
			}
		}
	}()
	return rvc, rvv, rve
}

type StatPoint struct {
	Time  int64
	Min   float64
	Mean  float64
	Max   float64
	Count uint64
}

func (b *BTrDBEndpoint) AlignedWindows(ctx context.Context, uu uuid.UUID, start int64, end int64, pointwidth uint8, version uint64) (chan StatPoint, chan uint64, chan error) {
	rv, err := b.g.AlignedWindows(ctx, &pb.AlignedWindowsParams{
		Uuid:         uu,
		Start:        start,
		End:          end,
		PointWidth:   uint32(pointwidth),
		VersionMajor: version,
	})
	rvc := make(chan StatPoint, 100)
	rvv := make(chan uint64, 1)
	rve := make(chan error, 1)
	wroteVer := false
	if err != nil {
		close(rvc)
		close(rvv)
		rve <- err
		close(rve)
		return rvc, rvv, rve
	}
	go func() {
		for {
			rawv, err := rv.Recv()
			if err == io.EOF {
				close(rvc)
				close(rvv)
				close(rve)
				return
			}
			if err != nil {
				close(rvc)
				close(rvv)
				rve <- err
				close(rve)
				return
			}
			if rawv.Stat != nil {
				close(rvc)
				close(rvv)
				rve <- &CodedError{rawv.Stat}
				close(rve)
				return
			}
			if !wroteVer {
				rvv <- rawv.VersionMajor
				wroteVer = true
			}
			for _, x := range rawv.Values {
				rvc <- StatPoint{
					Time:  x.Time,
					Min:   x.Min,
					Mean:  x.Mean,
					Max:   x.Max,
					Count: x.Count,
				}
			}
		}
	}()
	return rvc, rvv, rve
}
func (b *BTrDBEndpoint) Windows(ctx context.Context, uu uuid.UUID, start int64, end int64, width uint64, depth uint8, version uint64) (chan StatPoint, chan uint64, chan error) {
	rv, err := b.g.Windows(ctx, &pb.WindowsParams{
		Uuid:         uu,
		Start:        start,
		End:          end,
		Width:        width,
		Depth:        uint32(depth),
		VersionMajor: version,
	})
	rvc := make(chan StatPoint, 100)
	rvv := make(chan uint64, 1)
	rve := make(chan error, 1)
	wroteVer := false
	if err != nil {
		close(rvc)
		close(rvv)
		rve <- err
		close(rve)
		return rvc, rvv, rve
	}
	go func() {
		for {
			rawv, err := rv.Recv()
			if err == io.EOF {
				close(rvc)
				close(rvv)
				close(rve)
				return
			}
			if err != nil {
				close(rvc)
				close(rvv)
				rve <- err
				close(rve)
				return
			}
			if rawv.Stat != nil {
				close(rvc)
				close(rvv)
				rve <- &CodedError{rawv.Stat}
				close(rve)
				return
			}
			if !wroteVer {
				rvv <- rawv.VersionMajor
				wroteVer = true
			}
			for _, x := range rawv.Values {
				rvc <- StatPoint{
					Time:  x.Time,
					Min:   x.Min,
					Mean:  x.Mean,
					Max:   x.Max,
					Count: x.Count,
				}
			}
		}
	}()
	return rvc, rvv, rve
}

func (b *BTrDBEndpoint) DeleteRange(ctx context.Context, uu uuid.UUID, start int64, end int64) (uint64, error) {
	rv, err := b.g.Delete(ctx, &pb.DeleteParams{
		Uuid:  uu,
		Start: start,
		End:   end,
	})
	if err != nil {
		return 0, err
	}
	if rv.Stat != nil {
		return 0, &CodedError{rv.Stat}
	}
	return rv.VersionMajor, nil
}

func (b *BTrDBEndpoint) Info(ctx context.Context) (*MASH, error) {
	rv, err := b.g.Info(ctx, &pb.InfoParams{})
	if err != nil {
		return nil, err
	}
	if rv.Stat != nil {
		return nil, &CodedError{rv.Stat}
	}
	mrv := &MASH{rv.Mash, nil}
	mrv.precalculate()
	return mrv, nil
}

func (b *BTrDBEndpoint) Nearest(ctx context.Context, uu uuid.UUID, time int64, version uint64, backward bool) (RawPoint, uint64, error) {
	rv, err := b.g.Nearest(ctx, &pb.NearestParams{
		Uuid:         uu,
		Time:         time,
		VersionMajor: version,
		Backward:     backward,
	})
	if err != nil {
		return RawPoint{}, 0, err
	}
	if rv.Stat != nil {
		return RawPoint{}, 0, &CodedError{rv.Stat}
	}
	return RawPoint{Time: rv.Value.Time, Value: rv.Value.Value}, rv.VersionMajor, nil
}

func (b *BTrDBEndpoint) StreamInfo(ctx context.Context, uu uuid.UUID) (collection string, tags map[string]string, version uint64, err error) {
	rv, e := b.g.StreamInfo(ctx, &pb.StreamInfoParams{
		Uuid: uu,
	})
	if e != nil {
		err = e
		return
	}
	if rv.Stat != nil {
		err = &CodedError{rv.Stat}
		return
	}
	collection = rv.Collection
	version = rv.VersionMajor
	tags = make(map[string]string)
	for _, t := range rv.Tags {
		tags[t.Key] = t.Value
	}
	return
}

type ChangedRange struct {
	Version uint64
	Start   int64
	End     int64
}

func (b *BTrDBEndpoint) Changes(ctx context.Context, uu uuid.UUID, fromVersion uint64, toVersion uint64, resolution uint8) (chan ChangedRange, chan uint64, chan error) {
	rv, err := b.g.Changes(ctx, &pb.ChangesParams{
		Uuid:       uu,
		FromMajor:  fromVersion,
		ToMajor:    toVersion,
		Resolution: uint32(resolution),
	})
	rvc := make(chan ChangedRange, 100)
	rvv := make(chan uint64, 1)
	rve := make(chan error, 1)
	wroteVer := false
	if err != nil {
		close(rvc)
		close(rvv)
		rve <- err
		close(rve)
		return rvc, rvv, rve
	}
	go func() {
		for {
			cr, err := rv.Recv()
			if err == io.EOF {
				close(rvc)
				close(rvv)
				close(rve)
				return
			}
			if err != nil {
				close(rvc)
				close(rvv)
				rve <- err
				close(rve)
				return
			}
			if cr.Stat != nil {
				close(rvc)
				close(rvv)
				rve <- &CodedError{cr.Stat}
				close(rve)
				return
			}
			if !wroteVer {
				rvv <- cr.VersionMajor
				wroteVer = true
			}
			for _, x := range cr.Ranges {
				rvc <- ChangedRange{Version: cr.VersionMajor, Start: x.Start, End: x.End}
			}
		}
	}()
	return rvc, rvv, rve
}
