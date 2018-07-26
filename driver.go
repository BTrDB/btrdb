package btrdb

//go:generate protoc -I/usr/local/include -I. -I$GOPATH/src -I$GOPATH/src/github.com/grpc-ecosystem/grpc-gateway/third_party/googleapis --go_out=plugins=grpc:. grpcinterface/btrdb.proto
//go:generate protoc -I/usr/local/include -I. -I$GOPATH/src -I$GOPATH/src/github.com/grpc-ecosystem/grpc-gateway/third_party/googleapis --grpc-gateway_out=logtostderr=true:.  grpcinterface/btrdb.proto
//don't do this automagically protoc -I/usr/local/include -I. -I$GOPATH/src -I$GOPATH/src/github.com/grpc-ecosystem/grpc-gateway/third_party/googleapis --swagger_out=logtostderr=true:.  grpcinterface/btrdb.proto
import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/pborman/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	pb "gopkg.in/BTrDB/btrdb.v4/grpcinterface"
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

type apikeyCred string

func (a apikeyCred) GetRequestMetadata(ctx context.Context, uris ...string) (map[string]string, error) {
	return map[string]string{
		"authorization": fmt.Sprintf("bearer %s", a),
	}, nil
}

func (a apikeyCred) RequireTransportSecurity() bool {
	return false
}

//ConnectEndpoint is a low level call that connects to a single BTrDB
//server. It takes multiple arguments, but it is assumed that they are
//all different addresses for the same server, in decreasing order of
//priority. It returns a Endpoint, which is generally never used directly.
//Rather use Connect()
func ConnectEndpoint(ctx context.Context, addresses ...string) (*Endpoint, error) {
	return ConnectEndpointAuth(ctx, "", addresses...)
}

//ConnectEndpointAuth is a low level call that connects to a single BTrDB
//server. It takes multiple arguments, but it is assumed that they are
//all different addresses for the same server, in decreasing order of
//priority. It returns a Endpoint, which is generally never used directly.
//Rather use ConnectAuthenticated()
func ConnectEndpointAuth(ctx context.Context, apikey string, addresses ...string) (*Endpoint, error) {
	if len(addresses) == 0 {
		return nil, fmt.Errorf("No addresses provided")
	}
	ep_errors := ""
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
		addrport := strings.SplitN(a, ":", 2)
		if len(addrport) != 2 {
			fmt.Printf("invalid address:port %q\n", a)
			continue
		}
		secure := false
		if addrport[1] == "4411" {
			secure = true
		}
		dc := grpc.NewGZIPDecompressor()
		dialopts := []grpc.DialOption{
			grpc.WithTimeout(tmt),
			grpc.FailOnNonTempDialError(true),
			grpc.WithBlock(),
			grpc.WithDecompressor(dc),
			grpc.WithInitialWindowSize(1 * 1024 * 1024),
			grpc.WithInitialConnWindowSize(1 * 1024 * 1024)}

		if secure {
			dialopts = append(dialopts, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{})))
		} else {
			dialopts = append(dialopts, grpc.WithInsecure())
		}
		if apikey != "" {
			if !secure {
				fmt.Printf("WARNING: you are using API key authentication, but are using the insecure (unencrypted) API port. This is very dangerous and should never be done. Try connecting to port 4411 instead.")
			}
			dialopts = append(dialopts, grpc.WithPerRPCCredentials(apikeyCred(apikey)))
		}
		conn, err := grpc.Dial(a, dialopts...)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			ep_errors += fmt.Sprintf("endpoint error: err=%v a=%v\n", err, a)
			continue
		}
		client := pb.NewBTrDBClient(conn)
		rv := &Endpoint{g: client, conn: conn}
		return rv, nil
	}
	fmt.Printf(ep_errors)

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

//FaultInject is a debugging function that allows specific low level control of the endpoint.
//If you have to read the documentation, this is not for you. Server must be started with
//$BTRDB_ENABLE_FAULT_INJECT=YES
func (b *Endpoint) FaultInject(ctx context.Context, typ uint64, args []byte) ([]byte, error) {
	rv, err := b.g.FaultInject(ctx, &pb.FaultInjectParams{
		Type:   typ,
		Params: args,
	})
	if err != nil {
		return nil, err
	}
	if rv.GetStat() != nil {
		return nil, &CodedError{rv.GetStat()}
	}
	return rv.Rv, nil
}

//Create is a low level function, rather use BTrDB.Create()
func (b *Endpoint) Create(ctx context.Context, uu uuid.UUID, collection string, tags map[string]string, annotations map[string]string) error {
	taglist := []*pb.KeyValue{}
	for k, v := range tags {
		taglist = append(taglist, &pb.KeyValue{Key: k, Value: []byte(v)})
	}
	annlist := []*pb.KeyValue{}
	for k, v := range annotations {
		annlist = append(annlist, &pb.KeyValue{Key: k, Value: []byte(v)})
	}
	rv, err := b.g.Create(ctx, &pb.CreateParams{
		Uuid:        uu,
		Collection:  collection,
		Tags:        taglist,
		Annotations: annlist,
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
		Limit:     1000000,
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
func (b *Endpoint) StreamInfo(ctx context.Context, uu uuid.UUID, omitDescriptor bool, omitVersion bool) (
	collection string,
	aver AnnotationVersion,
	tags map[string]string,
	anns map[string]string,
	version uint64, err error) {
	rv, err := b.g.StreamInfo(ctx, &pb.StreamInfoParams{
		Uuid:           uu,
		OmitDescriptor: omitDescriptor,
		OmitVersion:    omitVersion})
	if err != nil {
		return "", 0, nil, nil, 0, err
	}
	if rv.GetStat() != nil {
		return "", 0, nil, nil, 0, &CodedError{rv.GetStat()}
	}
	if !omitDescriptor {
		tags = make(map[string]string)
		for _, kv := range rv.Descriptor_.Tags {
			tags[kv.Key] = string(kv.Value)
		}
		anns = make(map[string]string)
		for _, kv := range rv.Descriptor_.Annotations {
			anns[kv.Key] = string(kv.Value)
		}
		aver = AnnotationVersion(rv.Descriptor_.AnnotationVersion)
		collection = rv.Descriptor_.Collection
	}
	return collection, aver, tags, anns, rv.VersionMajor, nil
}

//SetStreamAnnotation is a low level function, rather use Stream.SetAnnotation() or Stream.CompareAndSetAnnotation()
func (b *Endpoint) SetStreamAnnotations(ctx context.Context, uu uuid.UUID, expected AnnotationVersion, changes map[string]*string) error {
	ch := []*pb.KeyOptValue{}
	for k, v := range changes {
		kop := &pb.KeyOptValue{
			Key: k,
		}
		if v != nil {
			kop.Val = &pb.OptValue{Value: []byte(*v)}
		}
		ch = append(ch, kop)
	}
	rv, err := b.g.SetStreamAnnotations(ctx, &pb.SetStreamAnnotationsParams{Uuid: uu, ExpectedAnnotationVersion: uint64(expected), Annotations: ch})
	if err != nil {
		return err
	}
	if rv.GetStat() != nil {
		return &CodedError{rv.GetStat()}
	}
	return nil
}

//GetMetadataUsage is a low level function. Rather use BTrDB.GetMetadataUsage
func (b *Endpoint) GetMetadataUsage(ctx context.Context, prefix string) (tags map[string]int, annotations map[string]int, err error) {
	rv, err := b.g.GetMetadataUsage(ctx, &pb.MetadataUsageParams{
		Prefix: prefix,
	})
	if err != nil {
		return nil, nil, err
	}
	if rv.Stat != nil {
		return nil, nil, &CodedError{rv.GetStat()}
	}
	tags = make(map[string]int)
	annotations = make(map[string]int)
	for _, kv := range rv.Tags {
		tags[kv.Key] = int(kv.Count)
	}
	for _, kv := range rv.Annotations {
		annotations[kv.Key] = int(kv.Count)
	}
	return tags, annotations, nil
}

//ListCollections is a low level function, and in particular has complex constraints. Rather use BTrDB.ListCollections()
func (b *Endpoint) ListCollections(ctx context.Context, prefix string, from string, limit uint64) ([]string, error) {
	if limit > 10000 {
		return nil, fmt.Errorf("Maxnumber must be <10k")
	}
	if limit == 0 {
		limit = 10000
	}
	rv, err := b.g.ListCollections(ctx, &pb.ListCollectionsParams{
		Prefix:    prefix,
		StartWith: from,
		Limit:     limit,
	})
	if err != nil {
		return nil, err
	}
	if rv.GetStat() != nil {
		return nil, &CodedError{rv.GetStat()}
	}
	return rv.Collections, nil
}

func streamFromLookupResult(lr *pb.StreamDescriptor, b *BTrDB) *Stream {
	rv := &Stream{
		uuid:              lr.Uuid,
		hasTags:           true,
		tags:              make(map[string]string),
		hasAnnotation:     true,
		annotations:       make(map[string]string),
		annotationVersion: AnnotationVersion(lr.AnnotationVersion),
		hasCollection:     true,
		collection:        lr.Collection,
		b:                 b,
	}
	for _, kv := range lr.Tags {
		rv.tags[kv.Key] = string(kv.Value)
	}
	for _, kv := range lr.Annotations {
		rv.annotations[kv.Key] = string(kv.Value)
	}
	return rv
}

//LookupStreams is a low level function, rather use BTrDB.LookupStreams()
func (b *Endpoint) LookupStreams(ctx context.Context, collection string, isCollectionPrefix bool, tags map[string]*string, annotations map[string]*string, patchDB *BTrDB) (chan *Stream, chan error) {
	ltags := []*pb.KeyOptValue{}
	for k, v := range tags {
		kop := &pb.KeyOptValue{
			Key: k,
		}
		if v != nil {
			kop.Val = &pb.OptValue{Value: []byte(*v)}
		}
		ltags = append(ltags, kop)
	}
	lanns := []*pb.KeyOptValue{}
	for k, v := range annotations {
		kop := &pb.KeyOptValue{
			Key: k,
		}
		if v != nil {
			kop.Val = &pb.OptValue{Value: []byte(*v)}
		}
		lanns = append(lanns, kop)
	}
	params := &pb.LookupStreamsParams{
		Collection:         collection,
		IsCollectionPrefix: isCollectionPrefix,
		Tags:               ltags,
		Annotations:        lanns,
	}
	// fmt.Printf("LUS PARAMS:\ncollection: %q, pfx %v\n", collection, isCollectionPrefix)
	// for _, tt := range ltags {
	// 	fmt.Printf("tag %q -> %p\n", tt.Key, tt.Val)
	// 	if tt.Val != nil {
	// 		fmt.Printf("  thats %q\n", string(tt.Val.Value))
	// 	}
	// }
	// for _, tt := range lanns {
	// 	fmt.Printf("tag %q -> %p\n", tt.Key, tt.Val)
	// 	if tt.Val != nil {
	// 		fmt.Printf("  thats %q\n", string(tt.Val.Value))
	// 	}
	// }
	rv, err := b.g.LookupStreams(ctx, params)
	rvc := make(chan *Stream, 100)
	rve := make(chan error, 1)
	if err != nil {
		close(rvc)
		rve <- err
		close(rve)
		return rvc, rve
	}
	go func() {
		for {
			lr, err := rv.Recv()
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
			if lr.Stat != nil {
				close(rvc)
				rve <- &CodedError{lr.Stat}
				close(rve)
				return
			}
			for _, r := range lr.Results {
				rvc <- streamFromLookupResult(r, patchDB)
			}
		}
	}()
	return rvc, rve
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

//Flush is a low level function, rather use Stream.Flush()
func (b *Endpoint) Flush(ctx context.Context, uu uuid.UUID) error {
	rv, err := b.g.Flush(ctx, &pb.FlushParams{
		Uuid: uu,
	})
	if err != nil {
		return err
	}
	if rv.Stat != nil {
		return &CodedError{rv.Stat}
	}
	return nil
}

//Obliterate is a low level function, rather use Stream.Obliterate()
func (b *Endpoint) Obliterate(ctx context.Context, uu uuid.UUID) error {
	rv, err := b.g.Obliterate(ctx, &pb.ObliterateParams{
		Uuid: uu,
	})
	if err != nil {
		return err
	}
	if rv.Stat != nil {
		return &CodedError{rv.Stat}
	}
	return nil
}

//Info is a low level function, rather use BTrDB.Info()
func (b *Endpoint) Info(ctx context.Context) (*MASH, *pb.InfoResponse, error) {
	rv, err := b.g.Info(ctx, &pb.InfoParams{})
	if err != nil {
		return nil, nil, err
	}
	if rv.Stat != nil {
		return nil, nil, &CodedError{rv.Stat}
	}
	mrv := &MASH{rv.Mash, nil}
	if rv.Mash != nil {
		mrv.precalculate()
	}
	return mrv, rv, nil
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
