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

	"github.com/pborman/uuid"
	pb "gopkg.in/BTrDB/btrdb.v4/grpcinterface"
)

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
	proxies   []string

	closed bool

	//This covers the epcache
	epmu    sync.RWMutex
	epcache map[uint32]*Endpoint

	bootstraps []string

	apikey string
}

func newBTrDB() *BTrDB {
	return &BTrDB{epcache: make(map[uint32]*Endpoint)}
}

//StatPoint represents a statistical summary of a window. The length of that
//window must be determined from context (e.g the parameters passed to AlignedWindow or Window methods)
type StatPoint struct {
	//The time of the start of the window, in nanoseconds since the epoch UTC
	Time  int64
	Min   float64
	Mean  float64
	Max   float64
	Count uint64
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
			fmt.Printf("attempt to connect to %s yielded %v\n", epa, err)
			continue
		}
		mash, inf, err := ep.Info(ctx)
		if err != nil {
			fmt.Printf("attempt to obtain MASH from %s yielded %v\n", epa, err)
			continue
		}
		if inf.GetProxy() != nil {
			//This is a proxied BTrDB cluster
			b.isproxied = true
			b.proxies = inf.GetProxy().GetProxyEndpoints()
		} else {
			b.activeMash.Store(mash)
		}
		ep.Disconnect()
		break
	}
	if (b.isproxied && len(b.proxies) == 0) ||
		(!b.isproxied && b.activeMash.Load() == nil) {
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
	b.epmu.RLock()
	ep, ok := b.epcache[hash]
	b.epmu.RUnlock()
	if ok {
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
		return nil, err
	}
	b.epmu.Lock()
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
		nep, err := ConnectEndpointAuth(ctx, b.apikey, b.proxies...)
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
	b.epmu.RLock()
	ep, ok := b.epcache[hash]
	b.epmu.RUnlock()
	if ok {
		return ep, nil
	}
	//We need to connect to endpoint
	nep, err := ConnectEndpointAuth(ctx, b.apikey, addrs...)
	if err != nil {
		return nil, err
	}
	b.epmu.Lock()
	b.epcache[hash] = nep
	b.epmu.Unlock()
	return nep, nil
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
		b.resyncProxies()
	} else {
		b.resyncInternalMash()
	}
}
func (b *BTrDB) resyncProxies() {
	b.epmu.Lock()
	defer b.epmu.Unlock()
	b.epcache = make(map[uint32]*Endpoint)
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
			b.epcache = make(map[uint32]*Endpoint)
			b.epmu.Unlock()
			return
		} else {
			fmt.Printf("error was %v", err)
		}
	}

	//Try bootstraps
	for _, epa := range b.bootstraps {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		ep, err := ConnectEndpointAuth(ctx, b.apikey, epa)
		cancel()
		if err != nil {
			fmt.Printf("attempt to connect to %s yielded %v\n", epa, err)
			continue
		}
		ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
		mash, _, err := ep.Info(ctx)
		cancel()
		if err != nil {
			fmt.Printf("attempt to obtain MASH from %s yielded %v\n", epa, err)
			continue
		}
		b.activeMash.Store(mash)
		b.epcache = make(map[uint32]*Endpoint)
		ep.Disconnect()
		b.epmu.Unlock()
		return
	}
	b.epmu.Unlock()
	//Try clients already in the MASH
	cm := b.activeMash.Load().(*MASH)
	for _, mbr := range cm.Members {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		ep, err := b.EndpointForHash(ctx, mbr.Hash)
		if err != nil {
			fmt.Printf("got errorb %v\n", err)
			continue
		}
		mash, _, err := ep.Info(ctx)
		cancel()
		if err == nil {
			b.activeMash.Store(mash)
			b.epcache = make(map[uint32]*Endpoint)
			return
		} else {
			fmt.Printf("got errorc %v", err)
		}
	}
	panic("No endpoints reachable!")
}

//This returns true if you should redo your operation (and get new ep)
//and false if you should return the last value/error you got
func (b *BTrDB) TestEpError(ep *Endpoint, err error) bool {
	if ep == nil && err == nil {
		return true
	}
	if err == forceEp {
		return true
	}
	if err == nil {
		return false
	}
	fmt.Printf("EP ERROR %v\n", err)
	if strings.Contains(err.Error(), "getsockopt: connection refused") {
		//why grpc no use proper code :(
		time.Sleep(300 * time.Millisecond)
		fmt.Printf("Got conn refused, resyncing silently\n")
		b.ResyncMash()
		return true
	}
	if strings.Contains(err.Error(), "Endpoint is unreachable on all addresses") {
		time.Sleep(300 * time.Millisecond)
		fmt.Printf("Got conn refused, resyncing silently\n")
		b.ResyncMash()
		return true
	}
	if grpc.Code(err) == codes.Unavailable {
		time.Sleep(300 * time.Millisecond)
		fmt.Printf("Got unavailable, resyncing mash silently\n")
		b.ResyncMash()
		return true
	}
	ce := ToCodedError(err)
	if ce.Code == 405 {
		time.Sleep(300 * time.Millisecond)
		fmt.Printf("Got 405 (wrong endpoint) silently retrying\n")
		b.ResyncMash()
		return true
	}
	if ce.Code == 419 {
		time.Sleep(300 * time.Millisecond)
		fmt.Printf("Got 419 (cluster degraded) silently retrying\n")
		b.ResyncMash()
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
			rv <- e
		}
		close(rv)
	}()
	return rv
}
