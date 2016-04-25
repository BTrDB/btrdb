package btrdb

import (
	"math"
	"testing"
	
	uuid "github.com/pborman/uuid"
)

var dbaddr string = "localhost:4410"

func floatEquals(x float64, y float64) bool {
	return math.Abs(x - y) < 1e-10 * math.Max(math.Abs(x), math.Abs(y))
}

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
	var stv *StatisticalValue
	var stvchan chan *StatisticalValue
	var asyncerr chan string
	var strerr string
	var i int
	
	var points []*StandardValue = make([]*StandardValue, 5)
	points[0] = &StandardValue{Time: 1, Value: 2.0}
	points[1] = &StandardValue{Time: 4, Value: 7.5}
	points[2] = &StandardValue{Time: 6, Value: 2.5}
	points[3] = &StandardValue{Time: 13, Value: 8.0}
	points[4] = &StandardValue{Time: 15, Value: 6.0}
	
	var expstatistical []*StatisticalValue = make([]*StatisticalValue, 3)
	expstatistical[0] = &StatisticalValue{Time: 0, Count: 1, Min: 2.0, Mean: 2.0, Max: 2.0}
	expstatistical[1] = &StatisticalValue{Time: 4, Count: 2, Min: 2.5, Mean: 5.0, Max: 7.5}
	expstatistical[2] = &StatisticalValue{Time: 12, Count: 2, Min: 6.0, Mean: 7.0, Max: 8.0}
	
	var expwindow []*StatisticalValue = make([]*StatisticalValue, 3)
	expwindow[0] = &StatisticalValue{Time: 0, Count: 2, Min: 2.0, Mean: 4.75, Max: 7.5}
	expwindow[1] = &StatisticalValue{Time: 5, Count: 1, Min: 2.5, Mean: 2.5, Max: 2.5}
	expwindow[2] = &StatisticalValue{Time: 10, Count: 1, Min: 8.0, Mean: 8.0, Max: 8.0}
	
	bc, err = NewBTrDBConnection(dbaddr)
	if err != nil {
		t.Fatal(err)
	}
	
	/* Insert Points */
	asyncerr, err = bc.InsertValues(uuid, points, true)
	strerr = <- asyncerr
	if err != nil || "ok" != strerr {
		t.Fatalf("Got unexpected error (%v, %s)", err, strerr)
	}
	
	/* Standard Values Query */
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
	
	/* Statistical Values Query */
	stvchan, _, asyncerr, err = bc.QueryStatisticalValues(uuid, 0, 16, 2, 0)
	if err != nil {
		t.Fatal(err)
	}
	
	for i = range expstatistical {
		stv = <- stvchan
		if stv == nil {
			t.Fatalf("Got nil point at index %d", i)
		} else if stv.Time != expstatistical[i].Time || stv.Count != expstatistical[i].Count || stv.Min != expstatistical[i].Min || !floatEquals(stv.Mean, expstatistical[i].Mean) || stv.Max != expstatistical[i].Max {
			t.Fatalf("Got incorrect point (time = %v, count = %v, min = %v, mean = %v, max d= %v) at index %d", stv.Time, stv.Count, stv.Min, stv.Mean, stv.Max, i)
		}
	}
	
	stv = <- stvchan
	if sv != nil {
		t.Fatalf("Got extra point (%v, %v)", sv.Time, sv.Value)
	}
	
	strerr = <- asyncerr
	if "" != strerr {
		t.Fatalf("Got unexpected error (got %s)", strerr)
	}
	
	
	/* Window Values Query */
	stvchan, _, asyncerr, err = bc.QueryWindowValues(uuid, 0, 16, 5, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	
	for i = range expwindow {
		stv = <- stvchan
		if stv == nil {
			t.Fatalf("Got nil point at index %d", i)
		} else if stv.Time != expwindow[i].Time || stv.Count != expwindow[i].Count || stv.Min != expwindow[i].Min || !floatEquals(stv.Mean, expwindow[i].Mean) || stv.Max != expwindow[i].Max {
			t.Fatalf("Got incorrect point (time = %v, count = %v, min = %v, mean = %v, max = %v) at index %d", stv.Time, stv.Count, stv.Min, stv.Mean, stv.Max, i)
		}
	}
	
	stv = <- stvchan
	if sv != nil {
		t.Fatalf("Got extra point (%v, %v)", sv.Time, sv.Value)
	}
	
	strerr = <- asyncerr
	if "" != strerr {
		t.Fatalf("Got unexpected error (got %s)", strerr)
	}
	
	/* Delete Values */
	asyncerr, err = bc.DeleteValues(uuid, 2, 8)
	strerr = <- asyncerr
	if err != nil || "ok" != strerr {
		t.Fatalf("Got unexpected error (%v, %s)", err, strerr)
	}
	
	points[1] = points[3]
	points[2] = points[4]
	points = points[:3]
	
	/* Standard Values Query (To Verify Deletion) */
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
