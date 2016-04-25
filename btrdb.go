package btrdb

import (
	"container/list"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	cpint "github.com/SoftwareDefinedBuildings/btrdb/cpinterface"
	capnp "github.com/glycerine/go-capnproto"
	uuid "github.com/pborman/uuid"
)

/* An infinite channel abstraction, used internally. */

type infchan struct {
	lock *sync.Mutex
	cond *sync.Cond
	queue *list.List
	open bool
}

func newInfChan() *infchan {
	var mutex *sync.Mutex = &sync.Mutex{}
	return &infchan{
		lock: mutex,
		cond: sync.NewCond(mutex),
		queue: list.New(),
		open: true,
	}
}

func (infc *infchan) enqueue(item interface{}) {
	infc.lock.Lock()
	defer infc.lock.Unlock()
	
	if !infc.open {
		panic("Attempting to enqueue into a closed infinite channel")
	}
	
	infc.queue.PushBack(item)
	if infc.queue.Len() == 1 {
		infc.cond.Signal()
	}
}

func (infc *infchan) dequeue() interface{} {
	infc.lock.Lock()
	
	defer infc.lock.Unlock()
	for infc.open && infc.queue.Len() == 0 {
		infc.cond.Wait()
	}
	
	if infc.queue.Len() != 0 {
		return infc.queue.Remove(infc.queue.Front())
	} else {
		return nil
	}
}

func (infc *infchan) close() {
	infc.lock.Lock()
	infc.open = false
	infc.cond.Broadcast()
	infc.lock.Unlock()
}

type BTrDBConnection struct {
	echotag uint64
	conn net.Conn
	connsendlock *sync.Mutex
	
	outstanding map[uint64]*infchan
	outstandinglock *sync.RWMutex
	
	open bool
}

func NewBTrDBConnection(addr string) (*BTrDBConnection, error) {
	var conn net.Conn
	var err error
	
	conn, err = net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	
	var bc *BTrDBConnection = &BTrDBConnection{
		echotag: 0,
		conn: conn,
		connsendlock: &sync.Mutex{},
		
		outstanding: make(map[uint64]*infchan),
		outstandinglock: &sync.RWMutex{},
		
		open: true,
	}
	
	go func() {
		var recvSeg *capnp.Segment
		var e error
		var response cpint.Response
		var respchan *infchan
		var et uint64
		for {
			recvSeg, e = capnp.ReadFromStream(conn, nil)
			if e != nil {
				if bc.open {
					fmt.Printf("Could not read response from BTrDB: %v\n", e)
				}
				return
			}
			response = cpint.ReadRootResponse(recvSeg)
			et = response.EchoTag()
			bc.outstandinglock.RLock()
			respchan = bc.outstanding[et]
			bc.outstandinglock.RUnlock()
			
			respchan.enqueue(response)
			
			if response.Final() {
				bc.outstandinglock.Lock()
				delete(bc.outstanding, et)
				bc.outstandinglock.Unlock()
				respchan.close()
			}
		}
	}()
	
	return bc, nil
}

func (bc *BTrDBConnection) newEchoTag() uint64 {
	return atomic.AddUint64(&bc.echotag, 1)
}

func (bc *BTrDBConnection) Close() error {
	bc.open = false
	return bc.conn.Close()
}

type StandardValue struct {
	Time int64
	Value float64
}

type StatisticalValue struct {
	Time int64
	Count uint64
	Min float64
	Mean float64
	Max float64
}

type TimeRange struct {
	StartTime int64
	EndTime int64
}

func (bc *BTrDBConnection) InsertValues(uuid uuid.UUID, points []*StandardValue, sync bool) (chan string, error) {
	var err error
	var et uint64 = bc.newEchoTag()
	var numrecs int = len(points)
	
	var seg *capnp.Segment = capnp.NewBuffer(nil)
	var req cpint.Request = cpint.NewRootRequest(seg)
	var query cpint.CmdInsertValues = cpint.NewCmdInsertValues(seg)
	var recList cpint.Record_List = cpint.NewRecordList(seg, numrecs)
	var pointList capnp.PointerList = capnp.PointerList(recList)
	var record cpint.Record = cpint.NewRecord(seg)
	
	var segments *infchan
	var asyncerr chan string
	
	var i int
	
	req.SetInsertValues(query)
	req.SetEchoTag(et)
	
	query.SetUuid(uuid)
	for i = 0; i < numrecs; i++ {
		record.SetTime(points[i].Time)
		record.SetValue(points[i].Value)
		pointList.Set(i, capnp.Object(record))
	}
	query.SetValues(recList)
	query.SetSync(sync)
	
	segments = newInfChan()
	bc.outstandinglock.Lock()
	bc.outstanding[et] = segments
	bc.outstandinglock.Unlock()
	
	bc.connsendlock.Lock()
	_, err = seg.WriteTo(bc.conn)
	bc.connsendlock.Unlock()
	
	if err != nil {
		bc.outstandinglock.Lock()
		delete(bc.outstanding, et)
		bc.outstandinglock.Unlock()
		return nil, err
	}
	
	asyncerr = make(chan string)
	
	go func () {
		defer close(asyncerr)
		
		for {
			var rawvalue interface{} = segments.dequeue()
			if rawvalue == nil {
				return
			}
			var response cpint.Response = rawvalue.(cpint.Response)
			var stat cpint.StatusCode = response.StatusCode()
			asyncerr <- stat.String()
		}
	}()
	
	return asyncerr, nil
}

func (bc *BTrDBConnection) DeleteValues(uuid uuid.UUID, start_time int64, end_time int64) (chan string, error) {
	var err error
	var et uint64 = bc.newEchoTag()
	
	var seg *capnp.Segment = capnp.NewBuffer(nil)
	var req cpint.Request = cpint.NewRootRequest(seg)
	var query cpint.CmdDeleteValues = cpint.NewCmdDeleteValues(seg)
	
	var segments *infchan
	var asyncerr chan string
	
	req.SetDeleteValues(query)
	req.SetEchoTag(et)
	
	query.SetUuid(uuid)
	query.SetStartTime(start_time)
	query.SetEndTime(end_time)
	
	segments = newInfChan()
	bc.outstandinglock.Lock()
	bc.outstanding[et] = segments
	bc.outstandinglock.Unlock()
	
	bc.connsendlock.Lock()
	_, err = seg.WriteTo(bc.conn)
	bc.connsendlock.Unlock()
	
	if err != nil {
		bc.outstandinglock.Lock()
		delete(bc.outstanding, et)
		bc.outstandinglock.Unlock()
		return nil, err
	}
	
	asyncerr = make(chan string)
	
	go func () {
		defer close(asyncerr)
		
		for {
			var rawvalue interface{} = segments.dequeue()
			if rawvalue == nil {
				return
			}
			var response cpint.Response = rawvalue.(cpint.Response)
			var stat cpint.StatusCode = response.StatusCode()
			asyncerr <- stat.String()
		}
	}()
	
	return asyncerr, nil
}

func (bc *BTrDBConnection) QueryStandardValues(uuid uuid.UUID, start_time int64, end_time int64, version uint64) (chan *StandardValue, chan uint64, chan string, error) {
	var err error
	var et uint64 = bc.newEchoTag()
	
	var seg *capnp.Segment = capnp.NewBuffer(nil)
	var req cpint.Request = cpint.NewRootRequest(seg)
	var query cpint.CmdQueryStandardValues = cpint.NewCmdQueryStandardValues(seg)
	
	var segments *infchan
	var rv chan *StandardValue
	var versionchan chan uint64
	var asyncerr chan string
	var sentversion bool
	
	req.SetQueryStandardValues(query)
	req.SetEchoTag(et)
	
	query.SetVersion(version)
	query.SetUuid(uuid)
	query.SetStartTime(start_time)
	query.SetEndTime(end_time)
	
	segments = newInfChan()
	bc.outstandinglock.Lock()
	bc.outstanding[et] = segments
	bc.outstandinglock.Unlock()
	
	bc.connsendlock.Lock()
	_, err = seg.WriteTo(bc.conn)
	bc.connsendlock.Unlock()
	
	if err != nil {
		bc.outstandinglock.Lock()
		delete(bc.outstanding, et)
		bc.outstandinglock.Unlock()
		return nil, nil, nil, err
	}
	
	rv = make(chan *StandardValue)
	versionchan = make(chan uint64, 1)
	asyncerr = make(chan string, 1)
	sentversion = false
	
	go func () {
		defer close(rv)
		defer close(asyncerr)
		defer func() {
			if !sentversion {
				close(versionchan)
			}
		}()
		
		for {
			var rawvalue interface{} = segments.dequeue()
			if rawvalue == nil {
				return
			}
			var response cpint.Response = rawvalue.(cpint.Response)
			var stat cpint.StatusCode = response.StatusCode()
			if stat != cpint.STATUSCODE_OK {
				asyncerr <- stat.String()
				return
			}
			var records cpint.Records = response.Records()
			if !sentversion {
				versionchan <- records.Version()
				close(versionchan)
				sentversion = true
			}
			
			var recordlist cpint.Record_List = records.Values()
			var length int = recordlist.Len()
			var i int
			
			for i = 0; i < length; i++ {
				var record cpint.Record = recordlist.At(i)
				rv <- &StandardValue{Time: record.Time(), Value: record.Value()}
			}
		}
	}()
	
	return rv, versionchan, asyncerr, nil
}

func (bc *BTrDBConnection) QueryNearestValue(uuid uuid.UUID, time int64, backward bool, version uint64) (chan *StandardValue, chan uint64, chan string, error) {
	var err error
	var et uint64 = bc.newEchoTag()
	
	var seg *capnp.Segment = capnp.NewBuffer(nil)
	var req cpint.Request = cpint.NewRootRequest(seg)
	var query cpint.CmdQueryNearestValue = cpint.NewCmdQueryNearestValue(seg)
	
	var segments *infchan
	var rv chan *StandardValue
	var versionchan chan uint64
	var asyncerr chan string
	var sentversion bool
	
	req.SetQueryNearestValue(query)
	req.SetEchoTag(et)
	
	query.SetVersion(version)
	query.SetUuid(uuid)
	query.SetTime(time)
	query.SetBackward(backward)
	
	segments = newInfChan()
	bc.outstandinglock.Lock()
	bc.outstanding[et] = segments
	bc.outstandinglock.Unlock()
	
	bc.connsendlock.Lock()
	_, err = seg.WriteTo(bc.conn)
	bc.connsendlock.Unlock()
	
	if err != nil {
		bc.outstandinglock.Lock()
		delete(bc.outstanding, et)
		bc.outstandinglock.Unlock()
		return nil, nil, nil, err
	}
	
	rv = make(chan *StandardValue)
	versionchan = make(chan uint64, 1)
	asyncerr = make(chan string, 1)
	sentversion = false
	
	go func () {
		defer close(rv)
		defer close(asyncerr)
		defer func() {
			if !sentversion {
				close(versionchan)
			}
		}()
		
		for {
			var rawvalue interface{} = segments.dequeue()
			if rawvalue == nil {
				return
			}
			var response cpint.Response = rawvalue.(cpint.Response)
			var stat cpint.StatusCode = response.StatusCode()
			if stat != cpint.STATUSCODE_OK {
				asyncerr <- stat.String()
				return
			}
			var records cpint.Records = response.Records()
			if !sentversion {
				versionchan <- records.Version()
				close(versionchan)
				sentversion = true
			}
			
			var recordlist cpint.Record_List = records.Values()
			var length int = recordlist.Len()
			var i int
			
			for i = 0; i < length; i++ {
				var record cpint.Record = recordlist.At(i)
				rv <- &StandardValue{Time: record.Time(), Value: record.Value()}
			}
		}
	}()
	
	return rv, versionchan, asyncerr, nil
}

func (bc *BTrDBConnection) QueryVersion(uuids []*uuid.UUID) (chan uint64, chan string, error) {
	var err error
	var et uint64 = bc.newEchoTag()
	var numrecs int = len(uuids)
	
	var seg *capnp.Segment = capnp.NewBuffer(nil)
	var req cpint.Request = cpint.NewRootRequest(seg)
	var query cpint.CmdQueryVersion = cpint.NewCmdQueryVersion(seg)
	var dataList capnp.DataList = seg.NewDataList(numrecs)
	
	var segments *infchan
	var rv chan uint64
	var asyncerr chan string
	
	var i int
	
	req.SetQueryVersion(query)
	req.SetEchoTag(et)
	
	for i = 0; i < numrecs; i++ {
		dataList.Set(i, *uuids[i])
	}
	query.SetUuids(dataList)
	
	segments = newInfChan()
	bc.outstandinglock.Lock()
	bc.outstanding[et] = segments
	bc.outstandinglock.Unlock()
	
	bc.connsendlock.Lock()
	_, err = seg.WriteTo(bc.conn)
	bc.connsendlock.Unlock()
	
	if err != nil {
		bc.outstandinglock.Lock()
		delete(bc.outstanding, et)
		bc.outstandinglock.Unlock()
		return nil, nil, err
	}
	
	rv = make(chan uint64)
	asyncerr = make(chan string, 1)
	
	go func () {
		defer close(rv)
		defer close(asyncerr)
		
		for {
			var rawvalue interface{} = segments.dequeue()
			if rawvalue == nil {
				return
			}
			var response cpint.Response = rawvalue.(cpint.Response)
			var stat cpint.StatusCode = response.StatusCode()
			if stat != cpint.STATUSCODE_OK {
				asyncerr <- stat.String()
				return
			}
			var records cpint.Versions = response.VersionList()
			
			var recordlist capnp.UInt64List = records.Versions()
			var length int = recordlist.Len()
			var i int
			
			for i = 0; i < length; i++ {
				rv <- recordlist.At(i)
			}
		}
	}()
	
	return rv, asyncerr, nil
}

func (bc *BTrDBConnection) QueryChangedRanges(uuid uuid.UUID, from_generation uint64, to_generation uint64, resolution uint8) (chan *TimeRange, chan uint64, chan string, error) {
	var err error
	var et uint64 = bc.newEchoTag()
	
	var seg *capnp.Segment = capnp.NewBuffer(nil)
	var req cpint.Request = cpint.NewRootRequest(seg)
	var query cpint.CmdQueryChangedRanges = cpint.NewCmdQueryChangedRanges(seg)
	
	var segments *infchan
	var rv chan *TimeRange
	var versionchan chan uint64
	var asyncerr chan string
	var sentversion bool
	
	req.SetQueryChangedRanges(query)
	req.SetEchoTag(et)
	
	query.SetUuid(uuid)
	query.SetFromGeneration(from_generation)
	query.SetToGeneration(to_generation)
	query.SetResolution(resolution)
	
	segments = newInfChan()
	bc.outstandinglock.Lock()
	bc.outstanding[et] = segments
	bc.outstandinglock.Unlock()
	
	bc.connsendlock.Lock()
	_, err = seg.WriteTo(bc.conn)
	bc.connsendlock.Unlock()
	
	if err != nil {
		bc.outstandinglock.Lock()
		delete(bc.outstanding, et)
		bc.outstandinglock.Unlock()
		return nil, nil, nil, err
	}
	
	rv = make(chan *TimeRange)
	versionchan = make(chan uint64, 1)
	asyncerr = make(chan string, 1)
	sentversion = false
	
	go func () {
		defer close(rv)
		defer close(asyncerr)
		defer func() {
			if !sentversion {
				close(versionchan)
			}
		}()
		
		for {
			var rawvalue interface{} = segments.dequeue()
			if rawvalue == nil {
				return
			}
			var response cpint.Response = rawvalue.(cpint.Response)
			var stat cpint.StatusCode = response.StatusCode()
			if stat != cpint.STATUSCODE_OK {
				asyncerr <- stat.String()
				return
			}
			var records cpint.Ranges = response.ChangedRngList()
			if !sentversion {
				versionchan <- records.Version()
				close(versionchan)
				sentversion = true
			}
			
			var recordlist cpint.ChangedRange_List = records.Values()
			var length int = recordlist.Len()
			var i int
			
			for i = 0; i < length; i++ {
				var record cpint.ChangedRange = recordlist.At(i)
				rv <- &TimeRange{StartTime: record.StartTime(), EndTime: record.EndTime()}
			}
		}
	}()
	
	return rv, versionchan, asyncerr, nil
}

func (bc *BTrDBConnection) QueryStatisticalValues(uuid uuid.UUID, start_time int64, end_time int64, point_width uint8, version uint64) (chan *StatisticalValue, chan uint64, chan string, error) {
	var err error
	var et uint64 = bc.newEchoTag()
	
	var seg *capnp.Segment = capnp.NewBuffer(nil)
	var req cpint.Request = cpint.NewRootRequest(seg)
	var query cpint.CmdQueryStatisticalValues = cpint.NewCmdQueryStatisticalValues(seg)
	
	var segments *infchan
	var rv chan *StatisticalValue
	var versionchan chan uint64
	var asyncerr chan string
	var sentversion bool
	
	req.SetQueryStatisticalValues(query)
	req.SetEchoTag(et)
	
	query.SetVersion(version)
	query.SetUuid(uuid)
	query.SetStartTime(start_time)
	query.SetEndTime(end_time)
	query.SetPointWidth(point_width)
	
	segments = newInfChan()
	bc.outstandinglock.Lock()
	bc.outstanding[et] = segments
	bc.outstandinglock.Unlock()
	
	bc.connsendlock.Lock()
	_, err = seg.WriteTo(bc.conn)
	bc.connsendlock.Unlock()
	
	if err != nil {
		bc.outstandinglock.Lock()
		delete(bc.outstanding, et)
		bc.outstandinglock.Unlock()
		return nil, nil, nil, err
	}
	
	rv = make(chan *StatisticalValue)
	versionchan = make(chan uint64, 1)
	asyncerr = make(chan string, 1)
	sentversion = false
	
	go func () {
		defer close(rv)
		defer close(asyncerr)
		defer func() {
			if !sentversion {
				close(versionchan)
			}
		}()
		
		for {
			var rawvalue interface{} = segments.dequeue()
			if rawvalue == nil {
				return
			}
			var response cpint.Response = rawvalue.(cpint.Response)
			var stat cpint.StatusCode = response.StatusCode()
			if stat != cpint.STATUSCODE_OK {
				asyncerr <- stat.String()
				return
			}
			var records cpint.StatisticalRecords = response.StatisticalRecords()
			if !sentversion {
				versionchan <- records.Version()
				close(versionchan)
				sentversion = true
			}
			
			var recordlist cpint.StatisticalRecord_List = records.Values()
			var length int = recordlist.Len()
			var i int
			
			for i = 0; i < length; i++ {
				var record cpint.StatisticalRecord = recordlist.At(i)
				rv <- &StatisticalValue{Time: record.Time(), Count: record.Count(), Min: record.Min(), Mean: record.Mean(), Max: record.Max()}
			}
		}
	}()
	
	return rv, versionchan, asyncerr, nil
}

func (bc *BTrDBConnection) QueryWindowValues(uuid uuid.UUID, start_time int64, end_time int64, width uint64, depth uint8, version uint64) (chan *StatisticalValue, chan uint64, chan string, error) {
	var err error
	var et uint64 = bc.newEchoTag()
	
	var seg *capnp.Segment = capnp.NewBuffer(nil)
	var req cpint.Request = cpint.NewRootRequest(seg)
	var query cpint.CmdQueryWindowValues = cpint.NewCmdQueryWindowValues(seg)
	
	var segments *infchan
	var rv chan *StatisticalValue
	var versionchan chan uint64
	var asyncerr chan string
	var sentversion bool
	
	req.SetQueryWindowValues(query)
	req.SetEchoTag(et)
	
	query.SetVersion(version)
	query.SetUuid(uuid)
	query.SetStartTime(start_time)
	query.SetEndTime(end_time)
	query.SetWidth(width)
	query.SetDepth(depth)
	
	segments = newInfChan()
	bc.outstandinglock.Lock()
	bc.outstanding[et] = segments
	bc.outstandinglock.Unlock()
	
	bc.connsendlock.Lock()
	_, err = seg.WriteTo(bc.conn)
	bc.connsendlock.Unlock()
	
	if err != nil {
		bc.outstandinglock.Lock()
		delete(bc.outstanding, et)
		bc.outstandinglock.Unlock()
		return nil, nil, nil, err
	}
	
	rv = make(chan *StatisticalValue)
	versionchan = make(chan uint64, 1)
	asyncerr = make(chan string, 1)
	sentversion = false
	
	go func () {
		defer close(rv)
		defer close(asyncerr)
		defer func() {
			if !sentversion {
				close(versionchan)
			}
		}()
		
		for {
			var rawvalue interface{} = segments.dequeue()
			if rawvalue == nil {
				return
			}
			var response cpint.Response = rawvalue.(cpint.Response)
			var stat cpint.StatusCode = response.StatusCode()
			if stat != cpint.STATUSCODE_OK {
				asyncerr <- stat.String()
				return
			}
			var records cpint.StatisticalRecords = response.StatisticalRecords()
			if !sentversion {
				versionchan <- records.Version()
				close(versionchan)
				sentversion = true
			}
			
			var recordlist cpint.StatisticalRecord_List = records.Values()
			var length int = recordlist.Len()
			var i int
			
			for i = 0; i < length; i++ {
				var record cpint.StatisticalRecord = recordlist.At(i)
				rv <- &StatisticalValue{Time: record.Time(), Count: record.Count(), Min: record.Min(), Mean: record.Mean(), Max: record.Max()}
			}
		}
	}()
	
	return rv, versionchan, asyncerr, nil
}
