package btrdb

import (
	"testing"
	
	uuid "github.com/pborman/uuid"
)

var dbaddr string = "localhost:4410"

func TestBadStream(t *testing.T) {
	var uuid uuid.UUID = uuid.NewRandom()
	var err error
	var bc *BTrDBConnection
	var sv *StandardValue
	var svchan chan *StandardValue
	var version uint64
	var versionchan chan uint64
	var asyncerr chan string
	var strerr string
	
	bc, err = NewBTrDBConnection(dbaddr)
	if err != nil {
		t.Fatal(err)
	}
	svchan, versionchan, asyncerr, err = bc.QueryStandardValues(uuid, 10, 20, 0)
	if err != nil {
		t.Fatal(err)
	}
	
	version = <- versionchan
	sv = <- svchan
	
	if version != 0 || sv != nil {
		t.Fatal("Got something")
	}
	
	strerr = <- asyncerr
	if "internalError" != strerr {
		t.Fatalf("Did not get expected error (got %s)", strerr)
	}
}

func TestSimpleInsert(t *testing.T) {
	var uuid uuid.UUID = uuid.NewRandom()
	var err error
	var bc *BTrDBConnection
	var sv *StandardValue
	var svchan chan *StandardValue
	var asyncerr chan string
	var strerr string
	var i int
	
	var points []*StandardValue = make([]*StandardValue, 4)
	points[0] = &StandardValue{Time: 1, Value: 2.0}
	points[1] = &StandardValue{Time: 4, Value: 7.5}
	points[2] = &StandardValue{Time: 6, Value: 2.5}
	points[3] = &StandardValue{Time: 13, Value: 8.0}
	
	bc, err = NewBTrDBConnection(dbaddr)
	if err != nil {
		t.Fatal(err)
	}
	
	asyncerr, err = bc.InsertValues(uuid, points, true)
	strerr = <- asyncerr
	if err != nil || "ok" != strerr {
		t.Fatalf("Got unexpected error (%v, %s)", err, strerr)
	}
	
	svchan, _, asyncerr, err = bc.QueryStandardValues(uuid, 0, 16, 0)
	if err != nil {
		t.Fatal(err)
	}
	
	for i = range points {
		sv = <- svchan
		if sv == nil {
			t.Fatalf("Got nil point at index %d", i)
		} else if sv.Time != points[i].Time || sv.Value != points[i].Value {
			t.Fatalf("Got incorrect point (%v, %v) at index %d", sv.Time, sv.Value, i)
		}
	}
	
	sv = <- svchan
	if sv != nil {
		t.Fatalf("Got extra point (%v, %v)", sv.Time, sv.Value)
	}
	
	strerr = <- asyncerr
	if "" != strerr {
		t.Fatalf("Got unexpected error (got %s)", strerr)
	}
}
