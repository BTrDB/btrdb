package btrdb

//go:generate protoc -I/usr/local/include -I. -I$GOPATH/src -I$GOPATH/src/github.com/grpc-ecosystem/grpc-gateway/third_party/googleapis --go_out=plugins=grpc:. v5api/btrdb.proto
//go:generate protoc -I/usr/local/include -I. -I$GOPATH/src -I$GOPATH/src/github.com/grpc-ecosystem/grpc-gateway/third_party/googleapis --grpc-gateway_out=logtostderr=true:.  v5api/btrdb.proto
//don't automatic go:generate protoc -I/usr/local/include -I. -I$GOPATH/src -I$GOPATH/src/github.com/grpc-ecosystem/grpc-gateway/third_party/googleapis --swagger_out=logtostderr=true:.  v5api/btrdb.proto
import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/pborman/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/BTrDB/btrdb/v5/bte"
	pb "github.com/BTrDB/btrdb/v5/v5api"
)

//PropertyVersion is the version of a stream annotations and tags. It begins at 1
//for a newly created stream and increases by 1 for each SetStreamAnnotation
//or SetStreamTags call. An PropertyVersion of 0 means "any version"
type PropertyVersion uint64

//How long we try to connect to an endpoint before trying the next one
const EndpointTimeout = 5 * time.Second

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
			if tmt > EndpointTimeout {
				tmt = EndpointTimeout
			}
		} else {
			tmt = EndpointTimeout
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
		securePorts := strings.Split(os.Getenv("BTRDB_SECURE_PORTS"),",")
		for _, portString := range securePorts {
			if addrport[1] == portString {
				secure = true
				break
			}
		}
		dc := grpc.NewGZIPDecompressor()
		dialopts := []grpc.DialOption{
			grpc.WithDecompressor(dc),
			grpc.WithTimeout(tmt),
			grpc.FailOnNonTempDialError(true),
			grpc.WithBlock()}

		if secure {
			dialopts = append(dialopts, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{})))
		} else {
			dialopts = append(dialopts, grpc.WithInsecure())
		}
		if apikey != "" {
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
		inf, err := client.Info(ctx, &pb.InfoParams{})
		if err != nil {
			ep_errors += fmt.Sprintf("endpoint error: err=%v a=%v\n", err, a)
			continue
		}
		if inf.MajorVersion != 5 {
			lg.Errorf("BTrDB server is the wrong version (expecting v5.x, got v%d.%d)", inf.MajorVersion, inf.MinorVersion)
			return nil, fmt.Errorf("Endpoint is the wrong version")
		}
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

func (b *Endpoint) GetClientConnection() *grpc.ClientConn {
	return b.conn
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
func (b *Endpoint) Create(ctx context.Context, uu uuid.UUID, collection string, tags map[string]*string, annotations map[string]*string) error {
	taglist := []*pb.KeyOptValue{}
	for k, v := range tags {
		if v == nil {
			taglist = append(taglist, &pb.KeyOptValue{Key: k})
		} else {
			taglist = append(taglist, &pb.KeyOptValue{Key: k, Val: &pb.OptValue{Value: *v}})
		}
	}
	annlist := []*pb.KeyOptValue{}
	for k, v := range annotations {
		if v == nil {
			annlist = append(annlist, &pb.KeyOptValue{Key: k})
		} else {
			annlist = append(annlist, &pb.KeyOptValue{Key: k, Val: &pb.OptValue{Value: *v}})
		}
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
func (b *Endpoint) ListAllCollections(ctx context.Context) (chan string, chan error) {
	return b.ListCollections(ctx, "")
}

//StreamInfo is a low level function, rather use Stream.Annotation()
func (b *Endpoint) StreamInfo(ctx context.Context, uu uuid.UUID, omitDescriptor bool, omitVersion bool) (
	collection string,
	pver PropertyVersion,
	tags map[string]*string,
	anns map[string]*string,
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
		tags = make(map[string]*string)
		for _, kv := range rv.Descriptor_.Tags {
			if kv.Val == nil {
				tags[kv.Key] = nil
			} else {
				vc := kv.Val.Value
				tags[kv.Key] = &vc
			}
		}
		anns = make(map[string]*string)
		for _, kv := range rv.Descriptor_.Annotations {
			if kv.Val == nil {
				anns[kv.Key] = nil
			} else {
				vc := kv.Val.Value
				anns[kv.Key] = &vc
			}
		}
		pver = PropertyVersion(rv.Descriptor_.PropertyVersion)
		collection = rv.Descriptor_.Collection
	}
	return collection, pver, tags, anns, rv.VersionMajor, nil
}

//SetStreamAnnotation is a low level function, rather use Stream.SetAnnotation() or Stream.CompareAndSetAnnotation()
func (b *Endpoint) SetStreamAnnotations(ctx context.Context, uu uuid.UUID, expected PropertyVersion, changes map[string]*string, remove []string) error {
	ch := []*pb.KeyOptValue{}
	for k, v := range changes {
		kop := &pb.KeyOptValue{
			Key: k,
		}
		if v != nil {
			kop.Val = &pb.OptValue{Value: *v}
		}
		ch = append(ch, kop)
	}
	rv, err := b.g.SetStreamAnnotations(ctx, &pb.SetStreamAnnotationsParams{Uuid: uu, ExpectedPropertyVersion: uint64(expected), Changes: ch, Removals: remove})
	if err != nil {
		return err
	}
	if rv.GetStat() != nil {
		return &CodedError{rv.GetStat()}
	}
	return nil
}

//SetStreamTags is a low level function, rather use Stream.SetTags()
func (b *Endpoint) SetStreamTags(ctx context.Context, uu uuid.UUID, expected PropertyVersion, collection string, changes map[string]*string) error {
	ch := []*pb.KeyOptValue{}
	for k, v := range changes {
		if v == nil {
			ch = append(ch, &pb.KeyOptValue{Key: k})
		} else {
			ch = append(ch, &pb.KeyOptValue{Key: k, Val: &pb.OptValue{Value: *v}})
		}
	}
	rv, err := b.g.SetStreamTags(ctx, &pb.SetStreamTagsParams{Uuid: uu, ExpectedPropertyVersion: uint64(expected), Tags: ch, Collection: collection})
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
func (b *Endpoint) ListCollections(ctx context.Context, prefix string) (chan string, chan error) {
	rv, err := b.g.ListCollections(ctx, &pb.ListCollectionsParams{
		Prefix: prefix,
	})
	rvc := make(chan string, 100)
	rve := make(chan error, 1)
	if err != nil {
		close(rvc)
		rve <- err
		close(rve)
		return rvc, rve
	}
	go func() {
		for {
			cols, err := rv.Recv()
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
			if cols.Stat != nil {
				close(rvc)
				rve <- &CodedError{cols.Stat}
				close(rve)
				return
			}
			for _, r := range cols.Collections {
				rvc <- r
			}
		}
	}()
	return rvc, rve
}

func streamFromLookupResult(lr *pb.StreamDescriptor, b *BTrDB) *Stream {
	rv := &Stream{
		uuid:            lr.Uuid,
		hasTags:         true,
		tags:            make(map[string]*string),
		hasAnnotation:   true,
		annotations:     make(map[string]*string),
		propertyVersion: PropertyVersion(lr.PropertyVersion),
		hasCollection:   true,
		collection:      lr.Collection,
		b:               b,
	}
	for _, kv := range lr.Tags {
		if kv.Val == nil {
			rv.tags[kv.Key] = nil
		} else {
			vc := kv.Val.Value
			rv.tags[kv.Key] = &vc
		}
	}
	for _, kv := range lr.Annotations {
		if kv.Val == nil {
			rv.annotations[kv.Key] = nil
		} else {
			vc := kv.Val.Value
			rv.annotations[kv.Key] = &vc
		}
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
			kop.Val = &pb.OptValue{Value: *v}
		}
		ltags = append(ltags, kop)
	}
	lanns := []*pb.KeyOptValue{}
	for k, v := range annotations {
		kop := &pb.KeyOptValue{
			Key: k,
		}
		if v != nil {
			kop.Val = &pb.OptValue{Value: *v}
		}
		lanns = append(lanns, kop)
	}
	params := &pb.LookupStreamsParams{
		Collection:         collection,
		IsCollectionPrefix: isCollectionPrefix,
		Tags:               ltags,
		Annotations:        lanns,
	}
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

//SQLQuery is a low level function, rather use BTrDB.SQLQuery()
func (b *Endpoint) SQLQuery(ctx context.Context, query string, params []string) (chan map[string]interface{}, chan error) {
	rv, err := b.g.SQLQuery(ctx, &pb.SQLQueryParams{
		Query:  query,
		Params: params,
	})
	rvc := make(chan map[string]interface{}, 100)
	rve := make(chan error, 1)
	if err != nil {
		close(rvc)
		rve <- err
		close(rve)
		return rvc, rve
	}
	go func() {
		for {
			row, err := rv.Recv()
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
			if row.Stat != nil {
				close(rvc)
				rve <- &CodedError{row.Stat}
				close(rve)
				return
			}
			for _, r := range row.SQLQueryRow {
				m := make(map[string]interface{})
				err := json.Unmarshal(r, &m)
				if err != nil {
					close(rvc)
					rve <- &CodedError{&pb.Status{Code: bte.BadSQLValue, Msg: "could not unmarshal SQL row"}}
					close(rve)
					return
				}
				rvc <- m
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
					Time:   x.Time,
					Min:    x.Min,
					Mean:   x.Mean,
					Max:    x.Max,
					Count:  x.Count,
					StdDev: x.Stddev,
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
					Time:   x.Time,
					Min:    x.Min,
					Mean:   x.Mean,
					Max:    x.Max,
					Count:  x.Count,
					StdDev: x.Stddev,
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

type ReducedResolutionRange struct {
	Start      int64
	End        int64
	Resolution uint32
}

type CompactionConfig struct {
	// Accessing versions LESS than this is not allowed
	CompactedVersion uint64
	// For every timestamp >= Start and < End in this list,
	// we cannot traverse the tree < Resolution.
	// These ranges are the new ones you want to add, not the full list
	ReducedResolutionRanges []*ReducedResolutionRange
	// Addresses less than this will be moved to the archive storage soon
	// You can't set this to less than it is, so zero means leave as is
	TargetArchiveHorizon uint64
}

//SetCompactionConfig is a low level function, use Stream.SetCompactionConfig instead
func (b *Endpoint) SetCompactionConfig(ctx context.Context, uu uuid.UUID, cfg *CompactionConfig) error {
	rrz := make([]*pb.ReducedResolutionRange, len(cfg.ReducedResolutionRanges))
	for i, r := range cfg.ReducedResolutionRanges {
		rrz[i] = &pb.ReducedResolutionRange{
			Start:      r.Start,
			End:        r.End,
			Resolution: r.Resolution,
		}
	}
	rv, err := b.g.SetCompactionConfig(ctx, &pb.SetCompactionConfigParams{
		Uuid:                    uu,
		CompactedVersion:        cfg.CompactedVersion,
		ReducedResolutionRanges: rrz,
		TargetArchiveHorizon:    cfg.TargetArchiveHorizon,
	})
	if err != nil {
		return err
	}
	if rv.Stat != nil {
		return &CodedError{rv.Stat}
	}
	return nil
}

//GetCompactionConfig is a low level function, use Stream.GetCompactionConfig instead
func (b *Endpoint) GetCompactionConfig(ctx context.Context, uu uuid.UUID) (cfg *CompactionConfig, majVersion uint64, err error) {
	rv, err := b.g.GetCompactionConfig(ctx, &pb.GetCompactionConfigParams{
		Uuid: uu,
	})
	if err != nil {
		return nil, 0, err
	}
	if rv.Stat != nil {
		return nil, 0, &CodedError{rv.Stat}
	}

	rrz := make([]*ReducedResolutionRange, len(rv.ReducedResolutionRanges))
	for i, r := range rv.ReducedResolutionRanges {
		rrz[i] = &ReducedResolutionRange{
			Start:      r.Start,
			End:        r.End,
			Resolution: r.Resolution,
		}
	}
	cfg = &CompactionConfig{
		CompactedVersion:        rv.CompactedVersion,
		ReducedResolutionRanges: rrz,
		TargetArchiveHorizon:    rv.TargetArchiveHorizon,
	}
	return cfg, rv.LatestMajorVersion, nil
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
