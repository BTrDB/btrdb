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
	var svchan chan StandardValue
	var version uint64
	var versionchan chan uint64
	var asyncerr chan string
	var strerr string
	var ok bool
	
	bc, err = NewBTrDBConnection(dbaddr)
	if err != nil {
		t.Fatal(err)
	}
	svchan, versionchan, asyncerr, err = bc.QueryStandardValues(uuid, 10, 20, 0)
	if err != nil {
		t.Fatal(err)
	}
	
	version = <- versionchan
	_, ok = <- svchan
	
	if version != 0 || ok {
		t.Fatal("Got something")
	}
	
	strerr = <- asyncerr
	if "internalError" != strerr {
		t.Fatalf("Did not get expected error (got %s)", strerr)
	}
}

func TestSimpleInsert(t *testing.T) {
	var myuuid uuid.UUID = uuid.NewRandom()
	var err error
	var bc *BTrDBConnection
	var oldversion uint64
	var newversion uint64
	var version uint64
	var versionchan chan uint64
	var sv StandardValue
	var svchan chan StandardValue
	var stv StatisticalValue
	var stvchan chan StatisticalValue
	var tr TimeRange
	var trchan chan TimeRange
	var asyncerr chan string
	var strerr string
	var i int
	var ok bool
	
	var points []StandardValue = make([]StandardValue, 5)
	points[0] = StandardValue{Time: 1, Value: 2.0}
	points[1] = StandardValue{Time: 4, Value: 7.5}
	points[2] = StandardValue{Time: 6, Value: 2.5}
	points[3] = StandardValue{Time: 13, Value: 8.0}
	points[4] = StandardValue{Time: 15, Value: 6.0}
	
	var expstatistical []StatisticalValue = make([]StatisticalValue, 3)
	expstatistical[0] = StatisticalValue{Time: 0, Count: 1, Min: 2.0, Mean: 2.0, Max: 2.0}
	expstatistical[1] = StatisticalValue{Time: 4, Count: 2, Min: 2.5, Mean: 5.0, Max: 7.5}
	expstatistical[2] = StatisticalValue{Time: 12, Count: 2, Min: 6.0, Mean: 7.0, Max: 8.0}
	
	var expwindow []StatisticalValue = make([]StatisticalValue, 3)
	expwindow[0] = StatisticalValue{Time: 0, Count: 2, Min: 2.0, Mean: 4.75, Max: 7.5}
	expwindow[1] = StatisticalValue{Time: 5, Count: 1, Min: 2.5, Mean: 2.5, Max: 2.5}
	expwindow[2] = StatisticalValue{Time: 10, Count: 1, Min: 8.0, Mean: 8.0, Max: 8.0}
	
	var uuidslice []uuid.UUID = make([]uuid.UUID, 1)
	uuidslice[0] = myuuid
	
	bc, err = NewBTrDBConnection(dbaddr)
	if err != nil {
		t.Fatal(err)
	}
	
	/* Insert Points */
	asyncerr, err = bc.InsertValues(myuuid, points, true)
	strerr = <- asyncerr
	if err != nil || "ok" != strerr {
		t.Fatalf("Got unexpected error (%v, %s)", err, strerr)
	}
	
	/* Standard Values Query */
	svchan, versionchan, asyncerr, err = bc.QueryStandardValues(myuuid, 0, 16, 0)
	if err != nil {
		t.Fatal(err)
	}
	
	for i = range points {
		sv, ok = <- svchan
		if !ok {
			t.Fatalf("Got nil point at index %d", i)
		} else if sv.Time != points[i].Time || sv.Value != points[i].Value {
			t.Fatalf("Got incorrect point (%v, %v) at index %d", sv.Time, sv.Value, i)
		}
	}
	
	sv, ok = <- svchan
	if ok {
		t.Fatalf("Got extra point (%v, %v)", sv.Time, sv.Value)
	}
	
	oldversion = <- versionchan
	
	/* Version Query, I */
	versionchan, asyncerr, err = bc.QueryVersion(uuidslice)
	if err != nil {
		t.Fatal(err)
	}
	
	version = <- versionchan
	if version != oldversion {
		t.Fatalf("Version query result (%v) does not match version number from previous query (%v)", version, oldversion)
	}
	
	version = <- versionchan
	if version != 0 {
		t.Fatalf("Got extra version (%v)", version)
	}
	
	strerr = <- asyncerr
	if "" != strerr {
		t.Fatalf("Got unexpected error (got %s)", strerr)
	}
	
	/* Nearest Value Query */
	svchan, _, asyncerr, err = bc.QueryNearestValue(myuuid, 12, true, 0)
	if err != nil {
		t.Fatal(err)
	}
	
	sv, ok = <- svchan
	if !ok {
		t.Fatal("Got nil point for nearest point")
	} else if sv.Time != points[2].Time || sv.Value != points[2].Value {
		t.Fatalf("Got incorrect nearest point (%v, %v)", sv.Time, sv.Value)
	}
	
	sv, ok = <- svchan
	if ok {
		t.Fatalf("Got extra point (%v, %v)", sv.Time, sv.Value)
	}
	
	strerr = <- asyncerr
	if "" != strerr {
		t.Fatalf("Got unexpected error (got %s)", strerr)
	}
	
	/* Statistical Values Query */
	stvchan, _, asyncerr, err = bc.QueryStatisticalValues(myuuid, 0, 16, 2, 0)
	if err != nil {
		t.Fatal(err)
	}
	
	for i = range expstatistical {
		stv, ok = <- stvchan
		if !ok {
			t.Fatalf("Got nil point at index %d", i)
		} else if stv.Time != expstatistical[i].Time || stv.Count != expstatistical[i].Count || stv.Min != expstatistical[i].Min || !floatEquals(stv.Mean, expstatistical[i].Mean) || stv.Max != expstatistical[i].Max {
			t.Fatalf("Got incorrect point (time = %v, count = %v, min = %v, mean = %v, max d= %v) at index %d", stv.Time, stv.Count, stv.Min, stv.Mean, stv.Max, i)
		}
	}
	
	stv, ok = <- stvchan
	if ok {
		t.Fatalf("Got extra point (time = %v, count = %v, min = %v, mean = %v, max d= %v)", stv.Time, stv.Count, stv.Min, stv.Mean, stv.Max)
	}
	
	strerr = <- asyncerr
	if "" != strerr {
		t.Fatalf("Got unexpected error (got %s)", strerr)
	}
	
	
	/* Window Values Query */
	stvchan, _, asyncerr, err = bc.QueryWindowValues(myuuid, 0, 14, 5, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	
	for i = range expwindow {
		stv, ok = <- stvchan
		if !ok {
			t.Fatalf("Got nil point at index %d", i)
		} else if stv.Time != expwindow[i].Time || stv.Count != expwindow[i].Count || stv.Min != expwindow[i].Min || !floatEquals(stv.Mean, expwindow[i].Mean) || stv.Max != expwindow[i].Max {
			t.Fatalf("Got incorrect point (time = %v, count = %v, min = %v, mean = %v, max = %v) at index %d", stv.Time, stv.Count, stv.Min, stv.Mean, stv.Max, i)
		}
	}
	
	stv, ok = <- stvchan
	if ok {
		t.Fatalf("Got extra point (time = %v, count = %v, min = %v, mean = %v, max d= %v)", stv.Time, stv.Count, stv.Min, stv.Mean, stv.Max)
	}
	
	strerr = <- asyncerr
	if "" != strerr {
		t.Fatalf("Got unexpected error (got %s)", strerr)
	}
	
	/* Delete Values */
	asyncerr, err = bc.DeleteValues(myuuid, 2, 8)
	strerr = <- asyncerr
	if err != nil || "ok" != strerr {
		t.Fatalf("Got unexpected error (%v, %s)", err, strerr)
	}
	
	points[1] = points[3]
	points[2] = points[4]
	points = points[:3]
	
	/* Standard Values Query (To Verify Deletion) */
	svchan, versionchan, asyncerr, err = bc.QueryStandardValues(myuuid, 0, 16, 0)
	if err != nil {
		t.Fatal(err)
	}
	
	for i = range points {
		sv, ok = <- svchan
		if !ok {
			t.Fatalf("Got nil point at index %d", i)
		} else if sv.Time != points[i].Time || sv.Value != points[i].Value {
			t.Fatalf("Got incorrect point (%v, %v) at index %d", sv.Time, sv.Value, i)
		}
	}
	
	sv, ok = <- svchan
	if ok {
		t.Fatalf("Got extra point (%v, %v)", sv.Time, sv.Value)
	}
	
	strerr = <- asyncerr
	if "" != strerr {
		t.Fatalf("Got unexpected error (got %s)", strerr)
	}
	
	newversion = <- versionchan
	
	if newversion <= oldversion {
		t.Fatalf("New version (%v) is less recent than the old version (%v)", newversion, oldversion)
	}
	
	/* Version Query, II */
	versionchan, asyncerr, err = bc.QueryVersion(uuidslice)
	if err != nil {
		t.Fatal(err)
	}
	
	version = <- versionchan
	if version != newversion {
		t.Fatalf("Version query result (%v) does not match version number from previous query (%v)", version, newversion)
	}
	
	version = <- versionchan
	if version != 0 {
		t.Fatalf("Got extra version (%v)", version)
	}
	
	strerr = <- asyncerr
	if "" != strerr {
		t.Fatalf("Got unexpected error (got %s)", strerr)
	}
	
	/* Changed Ranges Query, I */
	trchan, _, asyncerr, err = bc.QueryChangedRanges(myuuid, newversion, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	
	/* Should get back nothing since both ranges are the same. */
	tr, ok = <- trchan
	if ok {
		t.Fatalf("Got extra changed range [%v, %v]", tr.StartTime, tr.EndTime)
	}
	
	strerr = <- asyncerr
	if "" != strerr {
		t.Fatalf("Got unexpected error (got %s)", strerr)
	}
	
	/* Changed Ranges Query, II */
	trchan, _, asyncerr, err = bc.QueryChangedRanges(myuuid, oldversion, newversion, 0)
	if err != nil {
		t.Fatal(err)
	}
	
	/* Should get back one changed range. */
	tr, ok = <- trchan
	if !ok {
		t.Fatal("Got nil point for changed range")
	} else if tr.StartTime > 4 || tr.EndTime < 6 {
		t.Fatalf("Got incorrect changed range [%v, %v]", tr.StartTime, tr.EndTime)
	}
	
	tr, ok = <- trchan
	if ok {
		t.Fatalf("Got extra changed range (%v, %v)", tr.StartTime, tr.EndTime)
	}
	
	strerr = <- asyncerr
	if "" != strerr {
		t.Fatalf("Got unexpected error (got %s)", strerr)
	}
	
	err = bc.Close()
	if err != nil {
		t.Fatalf("Error while closing: %v", err)
	}
}
