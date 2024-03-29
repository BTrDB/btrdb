// Package btrdb implementes a golang client driver for btrdb
//
// For functions returning value, version and error channels, please pay attention
// to the following concurrency pattern:
//
//  - The value channel must be completely consumed, always.
//  - The version channel need not be consumed if not required. Only one value will ever be written to the version channel.
//  - The error channel need not be read, but you cannot assume that there  was not an error just because there were values
//  - You can defer reading the error channel until after the value channel is closed (it will be closed early on error).
//
// A good pattern is the following:
//   valchan, errchan = some.Method()
//   for v := range valchan {
//     do stuff
//   }
//   if err := <-errchan; err != nil {
//     handle error
//   }
package btrdb

import (
	"context"
	"strings"

	pb "github.com/BTrDB/btrdb/v5/v5api"
	"github.com/pborman/uuid"
)

// Maximum window of time that can be stored in a BTrDB tree
const (
	MinimumTime = -(16 << 56)
	MaximumTime = (48 << 56)
)

//OptKV is a utility function for use in SetAnnotations or LookupStreams that
//turns a list of arguments into a map[string]*string. Typical use:
//  OptKV("key","value", //Set or match key=vale
//        "key2", nil)   //Delete or match key2=*
//OptKV can also take a single map[string]string and return
//a map[string]*string, e.g
//  OptKV(stream.Tags()) //Match exactly this set of tags
func OptKV(iz ...interface{}) map[string]*string {
	if len(iz) == 1 {
		arg, ok := iz[0].(map[string]string)
		if !ok {
			panic("bad use of btrdb.OptKV: must have even number of arguments or a single map[string]string")
		}
		rv := make(map[string]*string)
		for k, v := range arg {
			cv := v
			rv[k] = &cv
		}
		return rv
	}
	if len(iz)%2 != 0 {
		panic("bad use of btrdb.OptKV: must have even number of arguments or a single map[string]string")
	}
	rv := make(map[string]*string)
	for i := 0; i < len(iz)/2; i++ {
		key, ok := iz[i*2].(string)
		if !ok {
			panic("bad use of btrdb.OptKV: even arguments must be string")
		}
		if iz[i*2+1] == nil {
			rv[key] = nil
		} else {
			val, ok := iz[i*2+1].(string)
			if !ok {
				panic("bad use of btrdb.OptKV: odd arguments must be string or nil")
			}
			rv[key] = &val
		}
	}
	return rv
}

//Stream is a handle on a Stream in BTrDB. Stream operations should be done through this object.
type Stream struct {
	b *BTrDB

	uuid uuid.UUID

	knownToExist bool

	hasTags bool
	tags    map[string]*string

	hasAnnotation   bool
	annotations     map[string]*string
	propertyVersion PropertyVersion

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

func (s *Stream) refreshMeta(ctx context.Context) error {
	var ep *Endpoint
	var pver PropertyVersion
	var err error
	var coll string
	var tags map[string]*string
	var anns map[string]*string
	for s.b.TestEpError(ep, err) {
		ep, err = s.b.ReadEndpointFor(ctx, s.uuid)
		if err != nil {
			continue
		}
		coll, pver, tags, anns, _, err = ep.StreamInfo(ctx, s.uuid, false, true)
		if err != nil {
			continue
		}
	}
	if err == nil {
		s.collection = coll
		s.hasCollection = true
		s.tags = tags
		s.hasTags = true
		s.knownToExist = true
		s.annotations = anns
		s.propertyVersion = pver
		s.hasAnnotation = true
		return nil
	}
	return err
}

//Exists returns true if the stream exists. This is essential after using
//StreamFromUUID as the stream may not exist, causing a 404 error on
//later stream operations. Any operation that returns a stream from
//collection and tags will have ensured the stream exists already.
func (s *Stream) Exists(ctx context.Context) (bool, error) {
	if s.knownToExist {
		return true, nil
	}
	err := s.refreshMeta(ctx)
	if err != nil && (ToCodedError(err).Code == 404 || strings.Contains(err.Error(), "[404] stream does not exist")) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

//UUID returns the stream's UUID. The stream may nor may not exist yet, depending
//on how the stream object was obtained. See also Stream.Exists()
func (s *Stream) UUID() uuid.UUID {
	return s.uuid
}

//Tags returns the tags of the stream. It may require a round trip to the
//server depending on how the stream was acquired. Do not modify the resulting
//map as it is a reference to the internal stream state
func (s *Stream) Tags(ctx context.Context) (map[string]*string, error) {
	if s.hasTags {
		return s.tags, nil
	}
	err := s.refreshMeta(ctx)
	if err != nil {
		return nil, err
	}
	return s.tags, nil
}

// If a stream has changed tags, you will need to call this to load the new tags
func (s *Stream) Refresh(ctx context.Context) error {
	return s.refreshMeta(ctx)
}

//Annotations returns the annotations of the stream (and the annotation version).
//It will always require a round trip to the server. If you are ok with stale
//data and want a higher performance version, use Stream.CachedAnnotations().
//Do not modify the resulting map.
func (s *Stream) Annotations(ctx context.Context) (map[string]*string, PropertyVersion, error) {
	err := s.refreshMeta(ctx)
	if err != nil {
		return nil, 0, err
	}
	return s.annotations, s.propertyVersion, nil
}

//CachedAnnotations returns the annotations of the stream, reusing previous
//results if available, otherwise fetching from the server
func (s *Stream) CachedAnnotations(ctx context.Context) (map[string]*string, PropertyVersion, error) {
	if !s.hasAnnotation {
		err := s.refreshMeta(ctx)
		if err != nil {
			return nil, 0, err
		}
	}
	return s.annotations, s.propertyVersion, nil
}

//Collection returns the collection of the stream. It may require a round
//trip to the server depending on how the stream was acquired
func (s *Stream) Collection(ctx context.Context) (string, error) {
	if s.hasCollection {
		return s.collection, nil
	}
	err := s.refreshMeta(ctx)
	if err != nil {
		return "", err
	}
	return s.collection, nil
}

//Version returns the current data version of the stream. This is not cached,
//it queries each time. Take care that you do not intorduce races in your
//code by assuming this function will always return the same vaue
func (s *Stream) Version(ctx context.Context) (uint64, error) {
	var ep *Endpoint
	var err error

	for s.b.TestEpError(ep, err) {
		ep, err = s.b.ReadEndpointFor(ctx, s.uuid)
		if err != nil {
			continue
		}
		var ver uint64
		_, _, _, _, ver, err = ep.StreamInfo(ctx, s.uuid, true, false)
		if err != nil {
			continue
		}
		return ver, nil
	}
	return 0, err
}

//Count the total number of points currently in the stream, efficiently.
func (s *Stream) Count(ctx context.Context, version uint64) (npoints uint64, err error) {
	points, _, echan := s.AlignedWindows(ctx, MinimumTime, MaximumTime, 62, version)
	for point := range points {
		npoints += point.Count
	}
	return npoints, <-echan
}

//InsertTV allows insertion of two equal length arrays, one containing times and
//the other containing values. The arrays need not be sorted, but they must correspond
//(i.e the first element of times is the time for the firt element of values). If the
//arrays are larger than appropriate, this function will automatically chunk the inserts.
//As a consequence, the insert is not necessarily atomic, but can be used with
//very large arrays.
func (s *Stream) InsertTV(ctx context.Context, times []int64, values []float64, p *InsertParams) error {
	if len(times) != len(values) {
		return ErrorWrongArgs
	}
	var ep *Endpoint
	var err error
	batchsize := 50000
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
		for s.b.TestEpError(ep, err) {
			ep, err = s.b.EndpointFor(ctx, s.uuid)
			if err != nil {
				continue
			}
			err = ep.Insert(ctx, s.uuid, pbraws, p)
		}
		if err != nil {
			return err
		}
		times = times[end:]
		values = values[end:]
	}
	return nil
}

//Flush writes the stream buffers out to persistent storage
func (s *Stream) Flush(ctx context.Context) error {
	var ep *Endpoint
	var err error
	for s.b.TestEpError(ep, err) {
		ep, err = s.b.EndpointFor(ctx, s.uuid)
		if err != nil {
			continue
		}
		err = ep.Flush(ctx, s.uuid)
	}
	if err != nil {
		return err
	}
	return nil
}

//Obliterate completely removes a stream. This operation is immediate but
//the space will only be freed slowly
func (s *Stream) Obliterate(ctx context.Context) error {
	var ep *Endpoint
	var err error
	for s.b.TestEpError(ep, err) {
		ep, err = s.b.EndpointFor(ctx, s.uuid)
		if err != nil {
			continue
		}
		err = ep.Obliterate(ctx, s.uuid)
	}
	if err != nil {
		return err
	}
	return nil
}

//CompareAndSetAnnotation will make the changes in the given map as long as the
//annotation version matches. To remove a key, specify it in the "remove" list
func (s *Stream) CompareAndSetAnnotation(ctx context.Context, expected PropertyVersion, changes map[string]*string, remove []string) error {
	var ep *Endpoint
	var err error
	for s.b.TestEpError(ep, err) {
		ep, err = s.b.EndpointFor(ctx, s.uuid)
		if err != nil {
			continue
		}
		err = ep.SetStreamAnnotations(ctx, s.uuid, expected, changes, remove)
	}
	if err != nil {
		return err
	}
	return nil
}

//CompareAndSetTags will update a stream's collection name and tags if the property version matches
func (s *Stream) CompareAndSetTags(ctx context.Context, expected PropertyVersion, collection string, changes map[string]*string) error {
	var ep *Endpoint
	var err error
	for s.b.TestEpError(ep, err) {
		ep, err = s.b.EndpointFor(ctx, s.uuid)
		if err != nil {
			continue
		}
		err = ep.SetStreamTags(ctx, s.uuid, expected, collection, changes)
	}
	if err != nil {
		return err
	}
	return nil
}

func (s *Stream) InsertGeneric(ctx context.Context, vals []RawPoint, p *InsertParams) error {
	var ep *Endpoint
	var err error
	batchsize := 50000
	for len(vals) > 0 {
		err = forceEp
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
		for s.b.TestEpError(ep, err) {
			ep, err = s.b.EndpointFor(ctx, s.uuid)
			if err != nil {
				continue
			}
			err = ep.InsertGeneric(ctx, s.uuid, pbraws, p)
		}
		if err != nil {
			return err
		}
		vals = vals[end:]
	}
	return nil

}

//InsertUnique acts like Insert, but allows specifying a merge policy.
func (s *Stream) InsertUnique(ctx context.Context, vals []RawPoint, mp MergePolicy) error {
	return s.InsertGeneric(ctx, vals, &InsertParams{MergePolicy: mp})
}

//Insert inserts the given array of RawPoint values. If the
//array is larger than appropriate, this function will automatically chunk the inserts.
//As a consequence, the insert is not necessarily atomic, but can be used with
//very large arrays.
func (s *Stream) Insert(ctx context.Context, vals []RawPoint) error {
	return s.InsertGeneric(ctx, vals, nil)
}

//InsertF will call the given time and val functions to get each value of the
//insertion. It is similar to InsertTV but may require less allocations if
//your data is already in a different data structure. If the
//size is larger than appropriate, this function will automatically chunk the inserts.
//As a consequence, the insert is not necessarily atomic, but can be used with
//very large size.
func (s *Stream) InsertF(ctx context.Context, length int, time func(int) int64, val func(int) float64, p *InsertParams) error {
	var ep *Endpoint
	var err error
	batchsize := 50000
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
		for s.b.TestEpError(ep, err) {
			ep, err = s.b.EndpointFor(ctx, s.uuid)
			if err != nil {
				continue
			}
			err = ep.Insert(ctx, s.uuid, pbraws, p)
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
	for s.b.TestEpError(ep, err) {
		ep, err = s.b.ReadEndpointFor(ctx, s.uuid)
		if err != nil {
			continue
		}
		rvchan, rvvchan, errchan := ep.RawValues(ctx, s.uuid, start, end, version)
		return rvchan, rvvchan, s.b.SnoopEpErr(ep, errchan)
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
//long, starting at start. Note that start is inclusive, but end is exclusive. That is, results will be returned for all windows that start
//in the interval [start, end). If end < start+2^pointwidth you will not get any results. If start and end are not powers of two, the bottom
//pointwidth bits will be cleared. Each window will contain statistical summaries of the window. Statistical points with count == 0 will be
//omitted.
func (s *Stream) AlignedWindows(ctx context.Context, start int64, end int64, pointwidth uint8, version uint64) (chan StatPoint, chan uint64, chan error) {
	var ep *Endpoint
	var err error
	for s.b.TestEpError(ep, err) {
		ep, err = s.b.ReadEndpointFor(ctx, s.uuid)
		if err != nil {
			continue
		}
		rvchan, rvvchan, errchan := ep.AlignedWindows(ctx, s.uuid, start, end, pointwidth, version)
		return rvchan, rvvchan, s.b.SnoopEpErr(ep, errchan)
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
//window will be width nanoseconds long. start is inclusive, but end is exclusive (e.g if end < start+width you will get no results). That is, results will
//be returned for all windows that start at a time less than the end timestamp. If (end - start) is not a multiple of width, then end will be decreased to
//the greatest value less than end such that (end - start) is a multiple of width (i.e., we set end = start + width * floordiv(end - start, width). The depth
//parameter is an optimization that can be used to speed up queries on fast queries. Each window will be accurate to 2^depth nanoseconds. If depth is zero,
//the results are accurate to the nanosecond. On a dense stream for large windows, this accuracy may not be required. For example for a window of a day, +- one
//second may be appropriate, so a depth of 30 can be specified. This is much faster to execute on the database side. The StatPoint channel MUST be fully
//consumed.
func (s *Stream) Windows(ctx context.Context, start int64, end int64, width uint64, depth uint8, version uint64) (chan StatPoint, chan uint64, chan error) {
	var ep *Endpoint
	var err error
	for s.b.TestEpError(ep, err) {
		ep, err = s.b.ReadEndpointFor(ctx, s.uuid)
		if err != nil {
			continue
		}
		rvchan, rvvchan, errchan := ep.Windows(ctx, s.uuid, start, end, width, depth, version)
		return rvchan, rvvchan, s.b.SnoopEpErr(ep, errchan)
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
	for s.b.TestEpError(ep, err) {
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
	for s.b.TestEpError(ep, err) {
		ep, err = s.b.ReadEndpointFor(ctx, s.uuid)
		if err != nil {
			continue
		}
		rv, ver, err = ep.Nearest(ctx, s.uuid, time, version, backward)
	}
	return
}

// Earliest returns the point nearest to the specified start time searching forward such that the
// returned point will be >= after. To find the earliest point that exists in a stream, use:
// stream.Earliest(context.Background(), btrdb.MinimumTime, 0).
func (s *Stream) Earliest(ctx context.Context, after int64, version uint64) (rv RawPoint, ver uint64, err error) {
	return s.Nearest(ctx, after, version, false)
}

// Latest returns the point nearest to the specified end time, searching backward such that the
// returned point will be < before. To find the latest point that exists in a stream, use:
// stream.Latest(context.Background(), btrdb.MaximumTime-1, 0). Another common usage is to find the
// point closest to now: stream.Latest(context.Background(), time.Now().UnixNano(), 0)
func (s *Stream) Latest(ctx context.Context, before int64, version uint64) (rv RawPoint, ver uint64, err error) {
	return s.Nearest(ctx, before, version, true)
}

// Changes returns the time intervals that have been altered between the two given versions. The precision of these time intervals is given by
// resolution (in log nanoseconds). The intervals will be rounded bigger if resolution is >0 but the calculation will be faster
func (s *Stream) Changes(ctx context.Context, fromVersion uint64, toVersion uint64, resolution uint8) (crv chan ChangedRange, cver chan uint64, cerr chan error) {
	var ep *Endpoint
	var err error
	for s.b.TestEpError(ep, err) {
		ep, err = s.b.ReadEndpointFor(ctx, s.uuid)
		if err != nil {
			continue
		}
		crchan, cvchan, errchan := ep.Changes(ctx, s.uuid, fromVersion, toVersion, resolution)
		return crchan, cvchan, s.b.SnoopEpErr(ep, errchan)
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

//GetCompactionConfig returns the compaction configuration for the given stream
func (s *Stream) GetCompactionConfig(ctx context.Context) (cfg *CompactionConfig, majVersion uint64, err error) {
	var ep *Endpoint
	for s.b.TestEpError(ep, err) {
		ep, err = s.b.EndpointFor(ctx, s.uuid)
		if err != nil {
			continue
		}
		cfg, majVersion, err = ep.GetCompactionConfig(ctx, s.uuid)
	}
	if err != nil {
		return nil, 0, err
	}
	return
}

//SetCompactionConfig sets the compaction configuration for the given stream
func (s *Stream) SetCompactionConfig(ctx context.Context, cfg *CompactionConfig) (err error) {
	var ep *Endpoint
	for s.b.TestEpError(ep, err) {
		ep, err = s.b.EndpointFor(ctx, s.uuid)
		if err != nil {
			continue
		}
		err = ep.SetCompactionConfig(ctx, s.uuid, cfg)
	}
	return
}

//Create a new stream with the given uuid, collection tags and annotations
func (b *BTrDB) Create(ctx context.Context, uu uuid.UUID, collection string, tags map[string]*string, annotations map[string]*string) (*Stream, error) {
	var ep *Endpoint
	var err error
	for b.TestEpError(ep, err) {
		ep, err = b.EndpointFor(ctx, uu)
		if err != nil {
			continue
		}
		err = ep.Create(ctx, uu, collection, tags, annotations)
	}
	if err != nil {
		return nil, err
	}
	rv := &Stream{
		uuid:            uu,
		collection:      collection,
		hasCollection:   true,
		tags:            make(map[string]*string),
		hasTags:         true,
		annotations:     make(map[string]*string),
		hasAnnotation:   true,
		propertyVersion: 0,
		b:               b,
	}
	//Copy the maps in case user messes with parameters
	for k, v := range tags {
		rv.tags[k] = v
	}
	for k, v := range annotations {
		rv.annotations[k] = v
	}
	return rv, nil
}

//ListCollections returns all collections on the server having the given prefix. It is preferable to use the streaming form
func (b *BTrDB) ListCollections(ctx context.Context, prefix string) ([]string, error) {
	rv := []string{}
	rvc, rve := b.StreamingListCollections(ctx, prefix)
	for v := range rvc {
		rv = append(rv, v)
	}
	if err := <-rve; err != nil {
		return nil, err
	}
	return rv, nil
}

//List all the collections with the given prefix, without loading them all into memory
func (b *BTrDB) StreamingListCollections(ctx context.Context, prefix string) (chan string, chan error) {
	var ep *Endpoint
	var err error
	for b.TestEpError(ep, err) {
		ep, err = b.GetAnyEndpoint(ctx)
		if err != nil {
			continue
		}
		streamchan, errchan := ep.ListCollections(ctx, prefix)
		return streamchan, b.SnoopEpErr(ep, errchan)
	}
	if err == nil {
		panic("Please report this")
	}
	rv := make(chan string)
	close(rv)
	errc := make(chan error, 1)
	errc <- err
	close(errc)
	return rv, errc
}

func (b *BTrDB) Info(ctx context.Context) (*MASH, error) {
	var ep *Endpoint
	var err error
	var rv *MASH
	for b.TestEpError(ep, err) {
		ep, err = b.GetAnyEndpoint(ctx)
		if err != nil {
			continue
		}
		rv, _, err = ep.Info(ctx)
	}
	return rv, err
}

func (b *BTrDB) StreamingLookupStreams(ctx context.Context, collection string, isCollectionPrefix bool, tags map[string]*string, annotations map[string]*string) (chan *Stream, chan error) {
	var ep *Endpoint
	var err error
	for b.TestEpError(ep, err) {
		ep, err = b.GetAnyEndpoint(ctx)
		if err != nil {
			continue
		}
		streamchan, errchan := ep.LookupStreams(ctx, collection, isCollectionPrefix, tags, annotations, b)
		return streamchan, b.SnoopEpErr(ep, errchan)
	}
	if err == nil {
		panic("Please report this")
	}
	rv := make(chan *Stream)
	close(rv)
	errc := make(chan error, 1)
	errc <- err
	close(errc)
	return rv, errc
}

//Execute a metadata SQL query but buffer the results in memory
func (b *BTrDB) SQLQuery(ctx context.Context, query string, params ...string) ([]map[string]interface{}, error) {
	rv := []map[string]interface{}{}
	cv, ce := b.StreamingSQLQuery(ctx, query, params...)
	for s := range cv {
		rv = append(rv, s)
	}
	if err := <-ce; err != nil {
		return nil, err
	}
	return rv, nil
}

//Execute a metadata SQL query
func (b *BTrDB) StreamingSQLQuery(ctx context.Context, query string, params ...string) (chan map[string]interface{}, chan error) {
	var ep *Endpoint
	var err error
	for b.TestEpError(ep, err) {
		ep, err = b.GetAnyEndpoint(ctx)
		if err != nil {
			continue
		}
		streamchan, errchan := ep.SQLQuery(ctx, query, params)
		return streamchan, b.SnoopEpErr(ep, errchan)
	}
	if err == nil {
		panic("Please report this")
	}
	rv := make(chan map[string]interface{})
	close(rv)
	errc := make(chan error, 1)
	errc <- err
	close(errc)
	return rv, errc
}

func (b *BTrDB) LookupStreams(ctx context.Context, collection string, isCollectionPrefix bool, tags map[string]*string, annotations map[string]*string) ([]*Stream, error) {
	rv := []*Stream{}
	cv, ce := b.StreamingLookupStreams(ctx, collection, isCollectionPrefix, tags, annotations)
	for s := range cv {
		rv = append(rv, s)
	}
	if err := <-ce; err != nil {
		return nil, err
	}
	return rv, nil
}

func (b *BTrDB) GetMetadataUsage(ctx context.Context, prefix string) (tags map[string]int, annotations map[string]int, err error) {
	var ep *Endpoint
	for b.TestEpError(ep, err) {
		ep, err = b.GetAnyEndpoint(ctx)
		if err != nil {
			continue
		}
		tags, annotations, err = ep.GetMetadataUsage(ctx, prefix)
	}
	return tags, annotations, err
}

// Execute a wide raw values query.
func (b *BTrDB) WValues(ctx context.Context, streams []*Stream, start int64, end int64, version uint64) (chan RawPointVec, chan uint64, chan error) {
	return b.WValuesAligned(ctx, streams, start, end, version, 0)
}

// Execute a wide raw value query with time alignment.
func (b *BTrDB) WValuesAligned(ctx context.Context, streams []*Stream, start int64, end int64, version uint64, period int64) (chan RawPointVec, chan uint64, chan error) {
	var ep *Endpoint
	var err error

	m := make(map[*Endpoint][]uuid.UUID)
	pvc := make(chan RawPointVec)
	rvc := make(chan uint64, 1)
	evc := make(chan error, 1)
	for _, s := range streams {
		for s.b.TestEpError(ep, err) {
			ep, err = s.b.ReadEndpointFor(ctx, s.uuid)
			if err != nil {
				continue
			}
		}
		m[ep] = append(m[ep], s.uuid)
	}
	for ep, ids := range m {
		ep.MultiRawValues(ctx, ids, pvc, rvc, evc, start, end, version, period)
	}
	return pvc, rvc, evc
}
