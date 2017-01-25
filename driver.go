package btrdb

//go:generate protoc grpcinterface/btrdb.proto --go_out=plugins=grpc:.
import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/pborman/uuid"
	"google.golang.org/grpc"
	pb "gopkg.in/btrdb.v4/grpcinterface"
)

//AnnotationVersion is the version of a stream annotation. It begins at 1
//for a newly created stream and increases by 1 for each SetStreamAnnotation
//call. An AnnotationVersion of 0 means "any version"
type AnnotationVersion uint64

//Endpoint is a low level connection to a single server. Rather use
//BTrDB which manages creating and destroying Endpoint objects as required
type Endpoint struct {
	g    pb.BTrDBClient
	conn *grpc.ClientConn
}

//RawPoint represents a single timestamped value
type RawPoint struct {
	//Nanoseconds since the epoch
	Time int64
	//Value. Units are stream-dependent
	Value float64
}

var forceEp = errors.New("Not really an error, you should not see this")

//ConnectEndpoint is a low level call that connects to a single BTrDB
//server. It takes multiple arguments, but it is assumed that they are
//all different addresses for the same server, in decreasing order of
//priority. It returns a Endpoint, which is generally never used directly.
//Rather use Connect()
func ConnectEndpoint(ctx context.Context, addresses ...string) (*Endpoint, error) {
	if len(addresses) == 0 {
		return nil, fmt.Errorf("No addresses provided")
	}
	for _, a := range addresses {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		dl, ok := ctx.Deadline()
		var tmt time.Duration
		if ok {
			tmt = dl.Sub(time.Now())
			if tmt > 2*time.Second {
				tmt = 2 * time.Second
			}
		} else {
			tmt = 2 * time.Second
		}

		conn, err := grpc.Dial(a, grpc.WithInsecure(), grpc.WithTimeout(tmt), grpc.FailOnNonTempDialError(true), grpc.WithBlock())
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			continue
		}
		client := pb.NewBTrDBClient(conn)
		rv := &Endpoint{g: client, conn: conn}
		return rv, nil
	}
	return nil, fmt.Errorf("Endpoint is unreachable on all addresses")
}

//GetGRPC will return the underlying GRPC client object.
func (b *Endpoint) GetGRPC() pb.BTrDBClient {
	return b.g
}

//Disconnect will close the underlying GRPC connection. The endpoint cannot be used
//after calling this method.
func (b *Endpoint) Disconnect() error {
	return b.conn.Close()
}

//Insert is a low level function, rather use Stream.Insert()
func (b *Endpoint) Insert(ctx context.Context, uu uuid.UUID, values []*pb.RawPoint) error {
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

//Create is a low level function, rather use BTrDB.Create()
func (b *Endpoint) Create(ctx context.Context, uu uuid.UUID, collection string, tags map[string]string, annotation []byte) error {
	taglist := []*pb.Tag{}
	for k, v := range tags {
		taglist = append(taglist, &pb.Tag{Key: k, Value: v})
	}
	rv, err := b.g.Create(ctx, &pb.CreateParams{
		Uuid:       uu,
		Collection: collection,
		Tags:       taglist,
		Annotation: annotation,
	})
	if err != nil {
		return err
	}
	if rv.GetStat() != nil {
		return &CodedError{rv.GetStat()}
	}
	return nil
}

//ListAllCollections is a low level function, and in particular will only work
//with small numbers of collections. Rather use BTrDB.ListAllCollections()
func (b *Endpoint) ListAllCollections(ctx context.Context) ([]string, error) {
	rv, err := b.g.ListCollections(ctx, &pb.ListCollectionsParams{
		Prefix:    "",
		StartWith: "",
		Number:    1000000,
	})
	if err != nil {
		return nil, err
	}
	if rv.GetStat() != nil {
		return nil, &CodedError{rv.GetStat()}
	}
	if len(rv.Collections) >= 1000000 {
		return nil, fmt.Errorf("You cannot use ListAllCollections if the number of collections exceeds 1 million. Please use ListCollections")
	}
	return rv.Collections, nil
}

//StreamAnnotation is a low level function, rather use Stream.Annotation()
func (b *Endpoint) StreamAnnotation(ctx context.Context, uu uuid.UUID) ([]byte, AnnotationVersion, error) {
	rv, err := b.g.StreamAnnotation(ctx, &pb.StreamAnnotationParams{Uuid: uu})
	if err != nil {
		return nil, 0, err
	}
	if rv.GetStat() != nil {
		return nil, 0, &CodedError{rv.GetStat()}
	}
	return rv.Annotation, AnnotationVersion(rv.AnnotationVersion), nil
}

//SetStreamAnnotation is a low level function, rather use Stream.SetAnnotation() or Stream.CompareAndSetAnnotation()
func (b *Endpoint) SetStreamAnnotation(ctx context.Context, uu uuid.UUID, expected AnnotationVersion, newVal []byte) error {
	rv, err := b.g.SetStreamAnnotation(ctx, &pb.SetStreamAnnotationParams{Uuid: uu, ExpectedAnnotationVersion: uint64(expected), Annotation: newVal})
	if err != nil {
		return err
	}
	if rv.GetStat() != nil {
		return &CodedError{rv.GetStat()}
	}
	return nil
}

//ListCollections is a low level function, and in particular has complex constraints. Rather use BTrDB.ListCollections()
func (b *Endpoint) ListCollections(ctx context.Context, prefix string, from string, maxnumber uint64) ([]string, error) {
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

//ListAllStreams is a low level function, rather use BTrDB.ListAllStreams()
func (b *Endpoint) ListAllStreams(ctx context.Context, collection string) ([]*Stream, error) {
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

//ListMatchingStreams is a low level function, rather use BTrDB.ListMatchingStreams()
func (b *Endpoint) ListMatchingStreams(ctx context.Context, collection string, tags map[string]string) ([]*Stream, error) {
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

//LookupStream is a low level function, rather use BTrDB.LookupStream()
func (b *Endpoint) LookupStream(ctx context.Context, collection string, tags map[string]string) (*Stream, error) {
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

//RawValues is a low level function, rather use Stream.RawValues()
func (b *Endpoint) RawValues(ctx context.Context, uu uuid.UUID, start int64, end int64, version uint64) (chan RawPoint, chan uint64, chan error) {
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

//AlignedWindows is a low level function, rather use Stream.AlignedWindows()
func (b *Endpoint) AlignedWindows(ctx context.Context, uu uuid.UUID, start int64, end int64, pointwidth uint8, version uint64) (chan StatPoint, chan uint64, chan error) {
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

//Windows is a low level function, rather use Stream.Windows()
func (b *Endpoint) Windows(ctx context.Context, uu uuid.UUID, start int64, end int64, width uint64, depth uint8, version uint64) (chan StatPoint, chan uint64, chan error) {
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

//DeleteRange is a low level function, rather use Stream.DeleteRange()
func (b *Endpoint) DeleteRange(ctx context.Context, uu uuid.UUID, start int64, end int64) (uint64, error) {
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

//Info is a low level function, rather use BTrDB.Info()
func (b *Endpoint) Info(ctx context.Context) (*MASH, error) {
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

//Nearest is a low level function, rather use Stream.Nearest()
func (b *Endpoint) Nearest(ctx context.Context, uu uuid.UUID, time int64, version uint64, backward bool) (RawPoint, uint64, error) {
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

//StreamInfo is a low level function, rather use Stream.Tags() Stream.Collection(), Stream.UUID() and Stream.Version() which handle caching
//where appropriate
func (b *Endpoint) StreamInfo(ctx context.Context, uu uuid.UUID) (collection string, tags map[string]string, version uint64, err error) {
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

//Changes is a low level function, rather use BTrDB.Changes()
func (b *Endpoint) Changes(ctx context.Context, uu uuid.UUID, fromVersion uint64, toVersion uint64, resolution uint8) (chan ChangedRange, chan uint64, chan error) {
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
