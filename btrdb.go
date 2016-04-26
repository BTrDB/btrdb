// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

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

// BTrDBConnection abstracts a single connection to a BTrDB. A single
// BTrDBConnection support multiple concurrent requests to BTrDB.
type BTrDBConnection struct {
	echotag uint64
	conn net.Conn
	connsendlock *sync.Mutex
	
	outstanding map[uint64]*infchan
	outstandinglock *sync.RWMutex
	
	open bool
}

// Creates a connection to the BTrDB at the provided address, and returns a
// BTrDBConnection to represent it.
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

// Releases the resources associated with this BTrDBConnection. It is undefined
// behavior to call this while there are outstanding requests on the
// connection.
func (bc *BTrDBConnection) Close() error {
	bc.open = false
	return bc.conn.Close()
}

// Represents a single data point.
type StandardValue struct {
	Time int64
	Value float64
}

// Represents statistics over a range of data.
type StatisticalValue struct {
	Time int64
	Count uint64
	Min float64
	Mean float64
	Max float64
}

// Represents an interval of time that is closed on the start and open on the
// end. In other words, represents [StartTime, EndTime).
type TimeRange struct {
	StartTime int64
	EndTime int64
}

// Inserts the points specified in the points slice into the stream
// corresponding to the specified UUID. If the sync parameter is true, then
// the BTrDB will commit the points to disk, before sending an acknowledgment;
// otherwise, it will return an acknowledgment and an OK will be received
// immediately, but the data will not be immediately queryable.
//
// Returns a channel with the status code of the operation, and an error in
// case the request could not be sent. When the acknowledgment is received from
// the BTrDB, the status code will appear on the channel as a string; 'ok' is
// the value corresponding to success.
func (bc *BTrDBConnection) InsertValues(uuid uuid.UUID, points []StandardValue, sync bool) (chan string, error) {
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

// Delete values from the specified stream in the specified time range.
//
// Return values are the same as in InsertValues.
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

// Makes a Standard Values Query for data in the specified stream, at the
// specified version, in the specified time range. A version number of 0
// means to use the latest version of the stream.
//
// Returns three channels and an error. The first channel contains the points
// satisfying the query. The second channel contains a single value, which is
// the version of the stream used to satisfy the query. In case BTrDB returns
// an error code, the third channel will contain a string describing the
// error (if the operation completes successfully, nothing is sent on this
// channel). The fourth value returned is an error, used if the request cannot
// be sent to the database.
func (bc *BTrDBConnection) QueryStandardValues(uuid uuid.UUID, start_time int64, end_time int64, version uint64) (chan StandardValue, chan uint64, chan string, error) {
	var err error
	var et uint64 = bc.newEchoTag()
	
	var seg *capnp.Segment = capnp.NewBuffer(nil)
	var req cpint.Request = cpint.NewRootRequest(seg)
	var query cpint.CmdQueryStandardValues = cpint.NewCmdQueryStandardValues(seg)
	
	var segments *infchan
	var rv chan StandardValue
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
	
	rv = make(chan StandardValue)
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
				rv <- StandardValue{Time: record.Time(), Value: record.Value()}
			}
		}
	}()
	
	return rv, versionchan, asyncerr, nil
}

// Makes a Nearest Value Query for the nearest point in the specified stream,
// at the specified version, nearest to the specified time, in the specified
// direction. A version number of 0 means to use the latest version of the
// stream.
//
// Return values are the same as in QueryStandardValues.
func (bc *BTrDBConnection) QueryNearestValue(uuid uuid.UUID, time int64, backward bool, version uint64) (chan StandardValue, chan uint64, chan string, error) {
	var err error
	var et uint64 = bc.newEchoTag()
	
	var seg *capnp.Segment = capnp.NewBuffer(nil)
	var req cpint.Request = cpint.NewRootRequest(seg)
	var query cpint.CmdQueryNearestValue = cpint.NewCmdQueryNearestValue(seg)
	
	var segments *infchan
	var rv chan StandardValue
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
	
	rv = make(chan StandardValue)
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
				rv <- StandardValue{Time: record.Time(), Value: record.Value()}
			}
		}
	}()
	
	return rv, versionchan, asyncerr, nil
}

// Makes a Version Query for the version corresponding to each of the streams
// specified by the provided UUIDs.
//
// Returns two channels and an error. The first channel contains the version
// numbers of the streams, in the same order as they were queried. In case
// BTrDB returns an error code, the third channel will contain a string
// describing the error (if the operation completes successfully, nothing is
// sent on this channel). The fourth value returned is an error, used if the
// request cannot be sent to the database.
func (bc *BTrDBConnection) QueryVersion(uuids []uuid.UUID) (chan uint64, chan string, error) {
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
		dataList.Set(i, uuids[i])
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

// Makes a Changed Ranges Query for ranges of time at the specified resolution
// that correspond to data that changed between the specified versions in the
// specified stream.
//
// Return values are the same as in QueryStandardValues.
func (bc *BTrDBConnection) QueryChangedRanges(uuid uuid.UUID, from_generation uint64, to_generation uint64, resolution uint8) (chan TimeRange, chan uint64, chan string, error) {
	var err error
	var et uint64 = bc.newEchoTag()
	
	var seg *capnp.Segment = capnp.NewBuffer(nil)
	var req cpint.Request = cpint.NewRootRequest(seg)
	var query cpint.CmdQueryChangedRanges = cpint.NewCmdQueryChangedRanges(seg)
	
	var segments *infchan
	var rv chan TimeRange
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
	
	rv = make(chan TimeRange)
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
				rv <- TimeRange{StartTime: record.StartTime(), EndTime: record.EndTime()}
			}
		}
	}()
	
	return rv, versionchan, asyncerr, nil
}

// Makes a Statistical Values Query. Data in the specified stream, at the
// specified version, in the specified time range, is broken up into time
// intervals, each 1 << point_width nanoseconds in size. For each time
// interval, a statistical aggregate of the data in the interval is generated
// containing the number of data points in the interval, and the minimum, mean,
// and maximum values of data in the interval. A version number of 0 means to
// use the latest version of the stream.
//
// Should the start and end times not line up with the sizes of the intervals,
// they are rounded down before making the query. The point_width parameter
// must be an integer in the interval [0, 62].
//
// Return values are the same as in QueryStandardValues.
func (bc *BTrDBConnection) QueryStatisticalValues(uuid uuid.UUID, start_time int64, end_time int64, point_width uint8, version uint64) (chan StatisticalValue, chan uint64, chan string, error) {
	var err error
	var et uint64 = bc.newEchoTag()
	
	var seg *capnp.Segment = capnp.NewBuffer(nil)
	var req cpint.Request = cpint.NewRootRequest(seg)
	var query cpint.CmdQueryStatisticalValues = cpint.NewCmdQueryStatisticalValues(seg)
	
	var segments *infchan
	var rv chan StatisticalValue
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
	
	rv = make(chan StatisticalValue)
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
				rv <- StatisticalValue{Time: record.Time(), Count: record.Count(), Min: record.Min(), Mean: record.Mean(), Max: record.Max()}
			}
		}
	}()
	
	return rv, versionchan, asyncerr, nil
}

// Makes a Statistical Values Query. Data in the specified stream, at the
// specified version, in the specified time range, is broken up into time
// intervals, of the specified width, in nanoseconds. For each time interval, a
// statistical aggregate of the data in the interval is generated containing
// the number of data points in the interval, and the minimum, mean, and
// maximum values of data in the interval. The endpoints of the intervals are
// precise up to 1 << depth nanoseconds; the query is more performant for
// larger values of depth. A version number of 0 means to use the latest
// version of the stream.
//
// Should the start and end times not line up with the sizes of the intervals,
// they are rounded down before making the query. The depth parameter must
// be an integer in the interval [0, 62].
//
// Return values are the same as in QueryStandardValues.
func (bc *BTrDBConnection) QueryWindowValues(uuid uuid.UUID, start_time int64, end_time int64, width uint64, depth uint8, version uint64) (chan StatisticalValue, chan uint64, chan string, error) {
	var err error
	var et uint64 = bc.newEchoTag()
	
	var seg *capnp.Segment = capnp.NewBuffer(nil)
	var req cpint.Request = cpint.NewRootRequest(seg)
	var query cpint.CmdQueryWindowValues = cpint.NewCmdQueryWindowValues(seg)
	
	var segments *infchan
	var rv chan StatisticalValue
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
	
	rv = make(chan StatisticalValue)
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
				rv <- StatisticalValue{Time: record.Time(), Count: record.Count(), Min: record.Min(), Mean: record.Mean(), Max: record.Max()}
			}
		}
	}()
	
	return rv, versionchan, asyncerr, nil
}
