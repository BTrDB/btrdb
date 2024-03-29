package btrdb

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	pb "github.com/BTrDB/btrdb/v5/v5api"
	logging "github.com/op/go-logging"
	"github.com/pborman/uuid"
)

var lg *logging.Logger

func init() {
	lg = logging.MustGetLogger("log")
}

//ErrorDisconnected is returned when operations are attempted after Disconnect()
//is called.
var ErrorDisconnected = &CodedError{&pb.Status{Code: 421, Msg: "Driver is disconnected"}}

//ErrorClusterDegraded is returned when a write operation on an unmapped UUID is attempted.
//generally the same operation will succeed if attempted once the cluster has recovered.
var ErrorClusterDegraded = &CodedError{&pb.Status{Code: 419, Msg: "Cluster is degraded"}}

//ErrorWrongArgs is returned from API functions if the parameters are nonsensical
var ErrorWrongArgs = &CodedError{&pb.Status{Code: 421, Msg: "Invalid Arguments"}}

//ErrorNoSuchStream is returned if an operation is attempted on a stream when
//it does not exist.
//var ErrorNoSuchStream = &CodedError{&pb.Status{Code: 404, Msg: "No such stream"}}

//BTrDB is the main object you should use to interact with BTrDB.
type BTrDB struct {
	//This covers the mash
	mashwmu    sync.Mutex
	activeMash atomic.Value

	isproxied bool

	closed bool

	//This covers the epcache
	epmu    sync.RWMutex
	epcache map[uint32]*Endpoint

	bootstraps []string

	apikey string

	//Must hold this when evaluating if an endpoint has failed and
	//requires a resync
	resyncMu sync.Mutex
	//Incremented every time there is a resync
	numResyncs int64
}

func newBTrDB() *BTrDB {
	return &BTrDB{epcache: make(map[uint32]*Endpoint)}
}

//StatPoint represents a statistical summary of a window. The length of that
//window must be determined from context (e.g the parameters passed to AlignedWindow or Window methods)
type StatPoint struct {
	//The time of the start of the window, in nanoseconds since the epoch UTC
	Time   int64
	Min    float64
	Mean   float64
	Max    float64
	Count  uint64
	StdDev float64
}

//Connect takes a list of endpoints and returns a BTrDB handle.
//Note that only a single endpoint is technically required, but having
//more endpoints will make the initial connection more robust to cluster
//changes. Different addresses for the same endpoint are permitted
func Connect(ctx context.Context, endpoints ...string) (*BTrDB, error) {
	return ConnectAuth(ctx, "", endpoints...)
}

//ConnectAuth takes an API key and a list of endpoints and returns a BTrDB handle.
//Note that only a single endpoint is technically required, but having
//more endpoints will make the initial connection more robust to cluster
//changes. Different addresses for the same endpoint are permitted
func ConnectAuth(ctx context.Context, apikey string, endpoints ...string) (*BTrDB, error) {
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("No endpoints provided")
	}
	b := newBTrDB()
	b.apikey = apikey
	b.bootstraps = endpoints
	for _, epa := range endpoints {
		ep, err := ConnectEndpointAuth(ctx, apikey, epa)
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if err != nil {
			lg.Warningf("attempt to connect to %s yielded %v", epa, err)
			continue
		}
		mash, inf, err := ep.Info(ctx)
		if err != nil {
			lg.Warningf("attempt to obtain MASH from %s yielded %v", epa, err)
			ep.Disconnect()
			continue
		}
		if inf.GetProxy() != nil {
			//This is a proxied BTrDB cluster
			b.isproxied = true
		} else {
			b.activeMash.Store(mash)
		}
		ep.Disconnect()
		break
	}
	if !b.isproxied && b.activeMash.Load() == nil {
		return nil, fmt.Errorf("Could not connect to cluster via provided endpoints")
	}
	return b, nil
}

//Disconnect will close all active connections to the cluster. All future calls
//will return ErrorDisconnected
func (b *BTrDB) Disconnect() error {
	b.epmu.Lock()
	defer b.epmu.Unlock()
	var gerr error
	for _, ep := range b.epcache {
		err := ep.Disconnect()
		if err != nil {
			gerr = err
		}
	}
	b.closed = true
	return gerr
}

//EndpointForHash is a low level function that returns a single endpoint for an
//endpoint hash.
func (b *BTrDB) EndpointForHash(ctx context.Context, hash uint32) (*Endpoint, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if b.isproxied {
		return b.EndpointFor(ctx, nil)
	}
	m := b.activeMash.Load().(*MASH)
	b.epmu.Lock()
	ep, ok := b.epcache[hash]
	if ok {
		b.epmu.Unlock()
		return ep, nil
	}
	var addrs []string
	for _, ep := range m.eps {
		if ep.hash == hash {
			addrs = ep.grpc
		}
	}
	//We need to connect to endpoint
	nep, err := ConnectEndpointAuth(ctx, b.apikey, addrs...)
	if err != nil {
		b.epmu.Unlock()
		return nil, err
	}
	b.epcache[hash] = nep
	b.epmu.Unlock()
	return nep, nil
}

//ReadEndpointFor returns the endpoint that should be used to read the given uuid
func (b *BTrDB) ReadEndpointFor(ctx context.Context, uuid uuid.UUID) (*Endpoint, error) {
	//TODO do rpref
	return b.EndpointFor(ctx, uuid)
}

//EndpointFor returns the endpoint that should be used to write the given uuid
func (b *BTrDB) EndpointFor(ctx context.Context, uuid uuid.UUID) (*Endpoint, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if b.isproxied {
		b.epmu.Lock()
		defer b.epmu.Unlock()
		for _, ep := range b.epcache {
			return ep, nil
		}
		//We need to connect to endpoint
		nep, err := ConnectEndpointAuth(ctx, b.apikey, b.bootstraps...)
		if err != nil {
			return nil, err
		}
		b.epcache[0] = nep
		return nep, nil
	}
	m := b.activeMash.Load().(*MASH)
	ok, hash, addrs := m.EndpointFor(uuid)
	if !ok {
		b.ResyncMash()
		return nil, ErrorClusterDegraded
	}
	b.epmu.Lock()
	ep, ok := b.epcache[hash]
	if ok {
		b.epmu.Unlock()
		return ep, nil
	}
	//We need to connect to endpoint
	nep, err := ConnectEndpointAuth(ctx, b.apikey, addrs...) //XX
	if err != nil {
		b.epmu.Unlock()
		return nil, err
	}
	b.epcache[hash] = nep
	b.epmu.Unlock()
	return nep, nil
}

func (b *BTrDB) dropEpcache() {
	for _, e := range b.epcache {
		e.Disconnect()
	}
	b.epcache = make(map[uint32]*Endpoint)
}

func (b *BTrDB) GetAnyEndpoint(ctx context.Context) (*Endpoint, error) {
	b.epmu.RLock()
	for _, ep := range b.epcache {
		b.epmu.RUnlock()
		return ep, nil
	}
	b.epmu.RUnlock()
	//Nothing in cache
	return b.EndpointFor(ctx, uuid.NewRandom())
}

func (b *BTrDB) ResyncMash() {
	if b.isproxied {
		b.resyncProxied()
	} else {
		b.resyncInternalMash()
	}
}
func (b *BTrDB) resyncProxied() {
	b.epmu.Lock()
	defer b.epmu.Unlock()
	b.dropEpcache()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	ep, err := ConnectEndpointAuth(ctx, b.apikey, b.bootstraps...)
	cancel()
	if err != nil {
		return
	}
	b.epcache[0] = ep
	return
}
func (b *BTrDB) resyncInternalMash() {
	b.epmu.Lock()

	for _, ep := range b.epcache {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		mash, _, err := ep.Info(ctx)
		cancel()
		if err == nil {
			b.activeMash.Store(mash)
			b.dropEpcache()
			b.epmu.Unlock()
			return
		} else {
			lg.Warningf("attempt to obtain info from endpoint cache error: %v", err)
		}
	}

	//Try bootstraps
	for _, epa := range b.bootstraps {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		ep, err := ConnectEndpointAuth(ctx, b.apikey, epa)
		cancel()
		if err != nil {
			lg.Warningf("attempt to connect to %s yielded %v", epa, err)
			continue
		}
		ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
		mash, _, err := ep.Info(ctx)
		cancel()
		if err != nil {
			ep.Disconnect()
			lg.Warningf("attempt to obtain MASH from %s yielded %v", epa, err)
			continue
		}
		b.activeMash.Store(mash)
		b.dropEpcache()
		ep.Disconnect()
		b.epmu.Unlock()
		return
	}
	b.epmu.Unlock()
	//Try clients already in the MASH
	cm := b.activeMash.Load().(*MASH)
	for _, mbr := range cm.Members {
		if !mbr.In {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		ep, err := b.EndpointForHash(ctx, mbr.Hash)
		if err != nil {
			lg.Warningf("obtaining endpoint for hash error: %v", err)
			cancel()
			continue
		}
		mash, _, err := ep.Info(ctx)
		cancel()
		if err == nil {
			b.activeMash.Store(mash)
			b.dropEpcache()
			return
		} else {
			lg.Warningf("obtaining endpoint info error: %v", err)
		}
	}
	lg.Warningf("failed to resync MASH, is BTrDB unavailable?")
}

//This returns true if you should redo your operation (and get new ep)
//and false if you should return the last value/error you got
func (b *BTrDB) TestEpError(ep *Endpoint, err error) bool {
	startNumResyncs := atomic.LoadInt64(&b.numResyncs)
	if ep == nil && err == nil {
		return true
	}
	if err == forceEp {
		return true
	}
	if err == nil {
		return false
	}

	mustResync := false

	if strings.Contains(err.Error(), "getsockopt: connection refused") {
		mustResync = true
		//why grpc no use proper code :(
		lg.Warningf("Got conn refused, resyncing silently")
	} else if strings.Contains(err.Error(), "Endpoint is unreachable on all addresses") {
		mustResync = true
		lg.Warningf("Got conn refused, resyncing silently")
	} else if grpc.Code(err) == codes.Unavailable {
		mustResync = true
		lg.Warningf("Got unavailable, resyncing mash silently")
	} else {
		ce := ToCodedError(err)
		if ce.Code == 405 {
			mustResync = true
			lg.Warningf("Got 405 (wrong endpoint) silently retrying")
		}
		if ce.Code == 419 {
			mustResync = true
			lg.Warningf("Got 419 (cluster degraded) silently retrying")
		}
	}

	if mustResync {
		//This is to avoid tight resync loops
		time.Sleep(300 * time.Millisecond)
		b.resyncMu.Lock()
		//No other goroutine has done a resync since we started this testEpError
		if b.numResyncs == startNumResyncs {
			b.ResyncMash()
			b.numResyncs++
		}
		b.resyncMu.Unlock()
		return true
	}
	return false
}

//This should invalidate the endpoint if some kind of error occurs.
//Because some values may have already been delivered, async functions using
//snoopEpErr will not be able to mask cluster errors from the user
func (b *BTrDB) SnoopEpErr(ep *Endpoint, err chan error) chan error {
	rv := make(chan error, 2)
	go func() {
		for e := range err {
			//if e is special invalidate ep
			b.TestEpError(ep, e)
			rv <- e
		}
		close(rv)
	}()
	return rv
}

//Subscriptions represent a set of
//real time subscriptions to streaming data.
type Subscriptions struct {
	err chan error
	id  []uuid.UUID
	c   chan SubRecord
}

//Data for a single stream returned from a Subscription.
type SubRecord struct {
	ID  uuid.UUID
	Val []RawPoint
}

//An endpoint and its associated uuids.
type EPGroup struct {
	*Endpoint
	ID []uuid.UUID
}

//EndpointSplit takes a variadic list of uuids and organizes them by the endpoint
//responsible for them.
func (b *BTrDB) EndpointsSplit(ctx context.Context, id ...uuid.UUID) ([]EPGroup, error) {
	var err error
	var ep *Endpoint
	who := make(map[string][]uuid.UUID)
	for b.TestEpError(ep, err) {
		ep, err = b.GetAnyEndpoint(ctx)
		if err != nil {
			continue
		}
		if b.isproxied {
			who["proxy"] = id
		} else {
			m := b.activeMash.Load().(*MASH)
			for i := range id {
				_, _, addrs := m.EndpointFor(id[i])
				if addrs == nil || len(addrs) != 1 {
					return nil, fmt.Errorf("can't find endpoint for %s", id[i])
				}
				who[addrs[0]] = append(who[addrs[0]], id[i])

			}
		}
	}

	var out []EPGroup
	i := 0
	for _, v := range who {
		ep, err := b.EndpointFor(ctx, v[0])
		for b.TestEpError(ep, err) {
			ep, err = b.EndpointFor(ctx, v[0])
		}
		if err != nil {
			return nil, err
		}
		out = append(out, EPGroup{ep, v})
		i++
	}
	return out, nil
}

//Subscribe takes a list of stream UUIDs to receive real time data points from.
//Connections are made to each relevant endpoints, and streams that belong to the
//same endpoint use the same connection. Data points are "raw", meaning they
//are given in the exact same sequence the database received them from the client.
func (b *BTrDB) Subscribe(ctx context.Context, id ...uuid.UUID) (*Subscriptions, error) {
	if len(id) == 0 {
		return nil, fmt.Errorf("no ids provided")
	}
	subs := &Subscriptions{
		id:  id,
		err: make(chan error),
		c:   make(chan SubRecord),
	}

	eps, err := b.EndpointsSplit(ctx, id...)
	if err != nil {
		return nil, err
	}
	for _, ep := range eps {
		ep.Endpoint.SubscribeTo(ctx, ep.ID, subs.c, subs.err)
	}
	return subs, nil
}

//Next gives either the most recent data for the set of subscriptions
//or an error regarding the connection state.
func (subs *Subscriptions) Next(ctx context.Context) (sr SubRecord, err error) {
	select {
	case <-ctx.Done():
		err = ctx.Err()
	case err = <-subs.err:
	case sr = <-subs.c:
	}
	return
}
