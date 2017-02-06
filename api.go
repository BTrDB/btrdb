// Package btrdb implementes a golang client driver for btrdb
//
// For functions returning value, version and error channels, please pay attention
// to the following concurrenct pattern:
// - The value channel must be completely consumed, always.
// - The version channel need not be consumed if not required. Only one value will ever be written to the version channel.
// - The error channel need not be read, but you cannot assume that there  was not an error just because there were values
// - You can defer reading the error channel until after the value channel is closed (it will be closed early on error).
// A good pattern is the following:
//   valchan, errchan = some.Method()
//   for v := range valchan {
//     do stuff
//   }
//   if <-errchan != nil {
//     handle error
//   }
package btrdb

import (
	"context"
	"fmt"

	"github.com/pborman/uuid"
	pb "gopkg.in/btrdb.v4/grpcinterface"
)

//Stream is a handle on a Stream in BTrDB. Stream operations should be done through this object.
type Stream struct {
	b *BTrDB

	uuid uuid.UUID

	hasTags bool
	tags    map[string]string

	hasCollection bool
	collection    string
}

//StreamFromUUID creates a stream handle for use in stream operations.
//it does not ensure that the stream exists, for that use Stream.Exists()
func (b *BTrDB) StreamFromUUID(uu uuid.UUID) *Stream {
	return &Stream{
		b:    b,
		uuid: uu,
	}
}

//Exists returns true if the stream exists. This is essential after using
//StreamFromUUID as the stream may not exist, causing a 404 error on
//later stream operations. Any operation that returns a stream from
//collection and tags will have ensured the stream exists already.
func (s *Stream) Exists(ctx context.Context) (bool, error) {

	var ep *Endpoint
	var err error
	var coll string
	var tags map[string]string
	for s.b.testEpError(ep, err) {
		ep, err = s.b.ReadEndpointFor(ctx, s.uuid)
		if err != nil {
			continue
		}
		coll, tags, _, err = ep.StreamInfo(ctx, s.uuid)
		if err != nil {
			if ToCodedError(err).Code == 404 {
				return false, nil
			}
			continue
		}
	}
	if err == nil {
		s.collection = coll
		s.hasCollection = true
		s.tags = tags
		s.hasTags = true
		return true, nil
	}
	return false, err
}

//UUID returns the stream's UUID. The stream may nor may not exist yet, depending
//on how the stream object was obtained. See also Stream.Exists()
func (s *Stream) UUID() uuid.UUID {
	return s.uuid
}

//Tags returns the tags of the stream. It may require a round trip to the
//server depending on how the stream was acquired.
func (s *Stream) Tags(ctx context.Context) (map[string]string, error) {
	if s.hasTags {
		return s.tags, nil
	}
	ex, err := s.Exists(ctx)
	if err != nil {
		return nil, err
	}
	if !ex {
		return nil, ErrorNoSuchStream
	}
	return s.tags, nil
}

//Collection returns the collection of the stream. It may require a round
//trip to the server depending on how the stream was acquired
func (s *Stream) Collection(ctx context.Context) (string, error) {
	if s.hasCollection {
		return s.collection, nil
	}
	ex, err := s.Exists(ctx)
	if err != nil {
		return "", err
	}
	if !ex {
		return "", ErrorNoSuchStream
	}
	return s.collection, nil
}

//Version returns the current version of the stream. This is not cached,
//it queries each time. Take care that you do not intorduce races in your
//code by assuming this function will always return the same vaue
func (s *Stream) Version(ctx context.Context) (uint64, error) {
	var ep *Endpoint
	var err error

	for s.b.testEpError(ep, err) {
		ep, err = s.b.ReadEndpointFor(ctx, s.uuid)
		if err != nil {
			continue
		}
		var coll string
		var tags map[string]string
		var ver uint64
		coll, tags, ver, err = ep.StreamInfo(ctx, s.uuid)
		if err != nil {
			continue
		}
		s.collection = coll
		s.hasCollection = true
		s.tags = tags
		s.hasTags = true
		return ver, nil
	}
	return 0, err
}

func (s *Stream) Annotation(ctx context.Context) (ann []byte, ver AnnotationVersion, err error) {
	var ep *Endpoint

	for s.b.testEpError(ep, err) {
		ep, err = s.b.ReadEndpointFor(ctx, s.uuid)
		if err != nil {
			continue
		}
		ann, ver, err = ep.StreamAnnotation(ctx, s.uuid)
	}
	return

}

//SetAnnotation sets an annotation on a BTrDB stream. An annotation is
//an arbitrary blob
func (s *Stream) SetAnnotation(ctx context.Context, ann []byte) error {
	var ep *Endpoint
	var err error
	for s.b.testEpError(ep, err) {
		ep, err = s.b.EndpointFor(ctx, s.uuid)
		if err != nil {
			continue
		}
		err = ep.SetStreamAnnotation(ctx, s.uuid, 0, ann)
	}
	return err
}

//InsertTV allows insertion of two equal length arrays, one containing times and
//the other containing values. The arrays need not be sorted, but they must correspond
//(i.e the first element of times is the time for the firt element of values). If the
//arrays are larger than appropriate, this function will automatically chunk the inserts.
//As a consequence, the insert is not necessarily atomic, but can be used with
//very large arrays.
func (s *Stream) InsertTV(ctx context.Context, times []int64, values []float64) error {
	if len(times) != len(values) {
		return ErrorWrongArgs
	}
	var ep *Endpoint
	var err error
	batchsize := 5000
	for len(times) > 0 {
		err = forceEp
		end := len(times)
		if end > batchsize {
			end = batchsize
		}
		thisBatchT := times[:end]
		thisBatchV := values[:end]
		//TODO pool or reuse
		pbraws := make([]*pb.RawPoint, len(thisBatchT))
		for i := 0; i < len(thisBatchT); i++ {
			pbraws[i] = &pb.RawPoint{
				Time:  thisBatchT[i],
				Value: thisBatchV[i],
			}
		}
		for s.b.testEpError(ep, err) {
			ep, err = s.b.EndpointFor(ctx, s.uuid)
			if err != nil {
				continue
			}
			err = ep.Insert(ctx, s.uuid, pbraws)
		}
		if err != nil {
			return err
		}
		times = times[end:]
		values = values[end:]
	}
	return nil
}

//Insert inserts the given array of RawPoint values. If the
//array is larger than appropriate, this function will automatically chunk the inserts.
//As a consequence, the insert is not necessarily atomic, but can be used with
//very large arrays.
func (s *Stream) Insert(ctx context.Context, vals []RawPoint) error {
	var ep *Endpoint
	var err error
	fmt.Printf("doing insert, vals len is %d\n", len(vals))
	batchsize := 5000
	for len(vals) > 0 {
		err = forceEp
		fmt.Printf("vals len is %d\n", len(vals))
		end := len(vals)
		if end > batchsize {
			end = batchsize
		}
		thisBatch := vals[:end]
		//TODO pool or reuse
		pbraws := make([]*pb.RawPoint, len(thisBatch))
		for idx, p := range thisBatch {
			pbraws[idx] = &pb.RawPoint{
				Time:  p.Time,
				Value: p.Value,
			}
		}
		for s.b.testEpError(ep, err) {
			ep, err = s.b.EndpointFor(ctx, s.uuid)
			if err != nil {
				continue
			}
			err = ep.Insert(ctx, s.uuid, pbraws)
			fmt.Printf("actually did insert, with err %v\n", err)
		}
		fmt.Printf("finished for loop, err is %v\n", err)
		if err != nil {
			return err
		}
		vals = vals[end:]
		fmt.Printf("after truncate val is %v\n", len(vals))
	}
	return nil
}

//InsertF will call the given time and val functions to get each value of the
//insertion. It is similar to InsertTV but may require less allocations if
//your data is already in a different data structure. If the
//size is larger than appropriate, this function will automatically chunk the inserts.
//As a consequence, the insert is not necessarily atomic, but can be used with
//very large size.
func (s *Stream) InsertF(ctx context.Context, length int, time func(int) int64, val func(int) float64) error {
	var ep *Endpoint
	var err error
	batchsize := 5000
	fidx := 0
	for fidx < length {
		err = forceEp
		tsize := length - fidx
		if tsize > batchsize {
			tsize = batchsize
		}
		//TODO pool or reuse
		pbraws := make([]*pb.RawPoint, tsize)
		for i := 0; i < tsize; i++ {
			pbraws[i] = &pb.RawPoint{
				Time:  time(fidx),
				Value: val(fidx),
			}
			fidx++
		}
		for s.b.testEpError(ep, err) {
			ep, err = s.b.EndpointFor(ctx, s.uuid)
			if err != nil {
				continue
			}
			err = ep.Insert(ctx, s.uuid, pbraws)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

//RawValues reads raw values from BTrDB. The returned RawPoint channel must be fully consumed.
func (s *Stream) RawValues(ctx context.Context, start int64, end int64, version uint64) (chan RawPoint, chan uint64, chan error) {
	var ep *Endpoint
	var err error
	for s.b.testEpError(ep, err) {
		ep, err = s.b.ReadEndpointFor(ctx, s.uuid)
		if err != nil {
			continue
		}
		rvchan, rvvchan, errchan := ep.RawValues(ctx, s.uuid, start, end, version)
		return rvchan, rvvchan, s.b.snoopEpErr(ep, errchan)
	}
	if err == nil {
		panic("Please report this")
	}
	rv := make(chan RawPoint)
	close(rv)
	rvv := make(chan uint64)
	close(rvv)
	errc := make(chan error, 1)
	errc <- err
	close(errc)
	return rv, rvv, errc
}

//AlignedWindows reads power-of-two aligned windows from BTrDB. It is faster than Windows(). Each returned window will be 2^pointwidth nanoseconds
//long, starting at start. Note that start is inclusive, but end is exclusive. If end <= start+2^pointwidth you will not get any results. If start
//and end are not powers of two, the bottom pointwidth bits will be cleared. Each window will contain statistical summaries of the window. For
//sections of time that do not contain results, it is possible that no records will be returned.
func (s *Stream) AlignedWindows(ctx context.Context, start int64, end int64, pointwidth uint8, version uint64) (chan StatPoint, chan uint64, chan error) {
	var ep *Endpoint
	var err error
	for s.b.testEpError(ep, err) {
		ep, err = s.b.ReadEndpointFor(ctx, s.uuid)
		if err != nil {
			continue
		}
		rvchan, rvvchan, errchan := ep.AlignedWindows(ctx, s.uuid, start, end, pointwidth, version)
		return rvchan, rvvchan, s.b.snoopEpErr(ep, errchan)
	}
	if err == nil {
		panic("Please report this")
	}
	rv := make(chan StatPoint)
	close(rv)
	rvv := make(chan uint64)
	close(rvv)
	errc := make(chan error, 1)
	errc <- err
	close(errc)
	return rv, rvv, errc
}

//Windows returns arbitrary precision windows from BTrDB. It is slower than AlignedWindows, but still significantly faster than RawValues. Each returned
//window will be width nanoseconds long. start is inclusive, but end is exclusive (e.g if end <= start+width you will get no results). The depth parameter
//is an optimization that can be used to speed up queries on fast queries. Each window will be accurate to 2^depth nanoseconds. If depth is zero, the results
//are accurate to the nanosecond. On a dense stream for large windows, this accuracy may not be required. For example for a window of a day, +- one second
//may be appropriate, so a depth of 30 can be specified. This is much faster to execute on the database side.
//The StatPoint channel MUST be fully consumed.
func (s *Stream) Windows(ctx context.Context, start int64, end int64, width uint64, depth uint8, version uint64) (chan StatPoint, chan uint64, chan error) {
	var ep *Endpoint
	var err error
	for s.b.testEpError(ep, err) {
		ep, err = s.b.ReadEndpointFor(ctx, s.uuid)
		if err != nil {
			continue
		}
		rvchan, rvvchan, errchan := ep.Windows(ctx, s.uuid, start, end, width, depth, version)
		return rvchan, rvvchan, s.b.snoopEpErr(ep, errchan)
	}
	if err == nil {
		panic("Please report this")
	}
	rv := make(chan StatPoint)
	close(rv)
	rvv := make(chan uint64)
	close(rvv)
	errc := make(chan error, 1)
	errc <- err
	close(errc)
	return rv, rvv, errc
}

//DeleteRange will delete all points between start (inclusive) and end (exclusive). Note that BTrDB has persistent
//multiversioning, so the deleted points can still be accessed on an older version of the stream
//returns the version of the stream and any error
func (s *Stream) DeleteRange(ctx context.Context, start int64, end int64) (ver uint64, err error) {
	var ep *Endpoint
	for s.b.testEpError(ep, err) {
		ep, err = s.b.EndpointFor(ctx, s.uuid)
		if err != nil {
			continue
		}
		ver, err = ep.DeleteRange(ctx, s.uuid, start, end)
	}
	return
}

//Nearest will return the nearest point to the given time. If backward is false, the returned point
//will be >= time. If backward is true, the returned point will be <time. The version of the
//stream used to satisfy the query is returned.
func (s *Stream) Nearest(ctx context.Context, time int64, version uint64, backward bool) (rv RawPoint, ver uint64, err error) {
	var ep *Endpoint
	for s.b.testEpError(ep, err) {
		ep, err = s.b.ReadEndpointFor(ctx, s.uuid)
		if err != nil {
			continue
		}
		rv, ver, err = ep.Nearest(ctx, s.uuid, time, version, backward)
	}
	return
}

func (s *Stream) Changes(ctx context.Context, fromVersion uint64, toVersion uint64, resolution uint8) (crv chan ChangedRange, cver chan uint64, cerr chan error) {
	var ep *Endpoint
	var err error
	for s.b.testEpError(ep, err) {
		ep, err = s.b.ReadEndpointFor(ctx, s.uuid)
		if err != nil {
			continue
		}
		crchan, cvchan, errchan := ep.Changes(ctx, s.uuid, fromVersion, toVersion, resolution)
		return crchan, cvchan, s.b.snoopEpErr(ep, errchan)
	}
	if err == nil {
		panic("Please report this")
	}
	rv := make(chan ChangedRange)
	close(rv)
	cver = make(chan uint64)
	close(cver)
	errc := make(chan error, 1)
	errc <- err
	close(errc)
	return rv, cver, errc
}

func (b *BTrDB) Create(ctx context.Context, uu uuid.UUID, collection string, tags map[string]string, annotation []byte) (*Stream, error) {
	var ep *Endpoint
	var err error
	for b.testEpError(ep, err) {
		ep, err = b.EndpointFor(ctx, uu)
		if err != nil {
			continue
		}
		err = ep.Create(ctx, uu, collection, tags, annotation)
	}
	if err != nil {
		return nil, err
	}
	rv := &Stream{
		uuid:          uu,
		collection:    collection,
		hasCollection: true,
		tags:          make(map[string]string),
		hasTags:       true,
		b:             b,
	}
	for k, v := range tags {
		rv.tags[k] = v
	}
	return rv, nil
}

func (b *BTrDB) ListAllCollections(ctx context.Context) ([]string, error) {
	return b.ListCollections(ctx, "")
}

func (b *BTrDB) ListCollections(ctx context.Context, prefix string) ([]string, error) {
	var ep *Endpoint
	var err error
	var rv []string
	from := ""
	//TODO change this to 10000 in future. This 10 just ensures code
	//coverage during initial alpha
	maximum := uint64(10)
	done := false
	for !done {
		var thisrv []string
		err = forceEp
		//Loop while errors are EP errors that will go away
		for b.testEpError(ep, err) {
			ep, err = b.getAnyEndpoint(ctx)
			if err != nil {
				continue
			}
			thisrv, err = ep.ListCollections(ctx, prefix, from, maximum)
		}
		//testEpError said stop trying, non-nil is fatal
		if err != nil {
			return nil, err
		}
		rv = append(rv, thisrv...)
		//We probably have more results
		if len(thisrv) == int(maximum) {
			from = thisrv[maximum-1]
		} else {
			//No more results
			done = true
		}
	}
	return rv, err
}

//ListAllStreams will return all the streams in the named collection
func (b *BTrDB) ListAllStreams(ctx context.Context, collection string) ([]*Stream, error) {
	var ep *Endpoint
	var err error
	var rv []*Stream
	for b.testEpError(ep, err) {
		ep, err = b.getAnyEndpoint(ctx)
		if err != nil {
			continue
		}
		rv, err = ep.ListAllStreams(ctx, collection)
	}
	for _, s := range rv {
		s.b = b
	}
	return rv, err
}

//ListMatchingStreams will return all streams in a collection that match the specified tags. If no tags are specified this
//method is equivalent to ListAllStreams()
func (b *BTrDB) ListMatchingStreams(ctx context.Context, collection string, tags map[string]string) ([]*Stream, error) {
	var ep *Endpoint
	var err error
	var rv []*Stream
	for b.testEpError(ep, err) {
		ep, err = b.getAnyEndpoint(ctx)
		if err != nil {
			continue
		}
		rv, err = ep.ListMatchingStreams(ctx, collection, tags)
	}
	return rv, err
}

//LookupStream takes a collection and a full set of tags and returns the resulting stream, or nil if the
//stream was not found. It is an error to omit any tags, even if the stream is uniquely identified by only
//a subset of the tags. To locate a stream with a unique subset of tags, use ListMatchingStreams.
func (b *BTrDB) LookupStream(ctx context.Context, collection string, tags map[string]string) (*Stream, error) {
	var ep *Endpoint
	var err error
	var rv *Stream
	for b.testEpError(ep, err) {
		ep, err = b.getAnyEndpoint(ctx)
		if err != nil {
			continue
		}
		rv, err = ep.LookupStream(ctx, collection, tags)
	}
	return rv, err
}

func (b *BTrDB) Info(ctx context.Context) (*MASH, error) {
	var ep *Endpoint
	var err error
	var rv *MASH
	for b.testEpError(ep, err) {
		ep, err = b.getAnyEndpoint(ctx)
		if err != nil {
			continue
		}
		rv, err = ep.Info(ctx)
	}
	return rv, err
}
