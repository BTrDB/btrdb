package tests2

import (
    "context"
    "fmt"
    "math"
    "math/rand"
    "testing"
    "time"

    "github.com/pborman/uuid"

    "github.com/SoftwareDefinedBuildings/btrdb/bte"
    "gopkg.in/btrdb.v4"
)

/* Helper functions. */

func helperConnect(t *testing.T, ctx context.Context) *btrdb.BTrDB {
    db, err := btrdb.Connect(ctx, btrdb.EndpointsFromEnv()...)
    if err != nil {
		t.Fatalf("Unexpected connection error: %v", err)
	}
    return db
}

func helperCreateDefaultStream(t *testing.T, ctx context.Context, db *btrdb.BTrDB, tags map[string]string, ann []byte) *btrdb.Stream {
    uu := uuid.NewRandom()
    coll := helperGetCollection(uu)
    s := helperCreateStream(t, ctx, db, uu, coll, tags, ann)
    suu := s.UUID()
    if len(suu) != len(uu) {
        t.Fatal("Bad UUID")
    }
    for i, b := range suu {
        if b != uu[i] {
            t.Fatal("UUID of created stream does not match provided UUID")
        }
    }
    return s
}

func helperGetCollection(uu uuid.UUID) string {
    return fmt.Sprintf("test.%x", uu[:])
}

func helperCreateStream(t *testing.T, ctx context.Context, db *btrdb.BTrDB, uu uuid.UUID, coll string, tags map[string]string, ann []byte) *btrdb.Stream {
    stream, err := db.Create(ctx, uu, coll, tags, ann)
    if err != nil {
		t.Fatalf("create error %v", err)
	}
    return stream
}

func helperWaitAfterInsert() {
    time.Sleep(12 * time.Second)
}

func helperInsert(t *testing.T, ctx context.Context, s *btrdb.Stream, data []btrdb.RawPoint) {
    err := s.Insert(ctx, data)
    if err != nil {
        t.Fatalf("insert error %v", err)
    }
    helperWaitAfterInsert()
}

func helperInsertTV(t *testing.T, ctx context.Context, s *btrdb.Stream, times []int64, values []float64) {
    err := s.InsertTV(ctx, times, values)
    if err != nil {
        t.Fatalf("insert error %v", err)
    }
    helperWaitAfterInsert()
}

func helperRandomData(start int64, end int64, gap int64) []btrdb.RawPoint {
    numpts := (end - start) / gap
    pts := make([]btrdb.RawPoint, numpts)
    for i, _ := range pts {
        pts[i].Time = start + (int64(i) * gap)
        pts[i].Value = rand.NormFloat64()
    }
    return pts
}

func helperRandomDataCount(start int64, end int64, numpts int64) []btrdb.RawPoint {
    gap := (end - start) / numpts
    return helperRandomData(start, end, gap)
}

func helperRawQuery(t *testing.T, ctx context.Context, s *btrdb.Stream, start int64, end int64, version uint64) ([]btrdb.RawPoint, uint64) {
    rpc, verc, errc := s.RawValues(ctx, start, end, version)
    rv := make([]btrdb.RawPoint, 0)
    for rp := range rpc {
        rv = append(rv, rp)
    }
    ver := <-verc
    err := <-errc
    if err != nil {
        t.Fatalf("raw query error: %v", err)
    }

    return rv, ver
}

func helperWindowQuery(t *testing.T, ctx context.Context, s *btrdb.Stream, start int64, end int64, width uint64, depth uint8, version uint64) ([]btrdb.StatPoint, uint64) {
    spc, verc, errc := s.Windows(ctx, start, end, width, depth, version)
    rv := make([]btrdb.StatPoint, 0)
    for sp := range spc {
        rv = append(rv, sp)
    }
    ver := <-verc
    err := <-errc
    if err != nil {
        t.Fatalf("window query error: %v", err)
    }

    return rv, ver
}

func helperStatisticalQuery(t *testing.T, ctx context.Context, s *btrdb.Stream, start int64, end int64, pwe uint8, version uint64) ([]btrdb.StatPoint, uint64) {
    spc, verc, errc := s.AlignedWindows(ctx, start, end, pwe, version)
    rv := make([]btrdb.StatPoint, 0)
    for sp := range spc {
        rv = append(rv, sp)
    }
    ver := <-verc
    err := <-errc
    if err != nil {
        t.Fatalf("statistical query error: %v", err)
    }

    return rv, ver
}

const CANONICAL_END int64 = 1000000000000000000
const CANONICAL_START int64 = 100
const CANONICAL_COUNT int = 10000
const CANONICAL_FINAL int64 = CANONICAL_START + int64(CANONICAL_COUNT - 1) * ((CANONICAL_END - CANONICAL_START) / int64(CANONICAL_COUNT))
func helperCanonicalData() []btrdb.RawPoint {
    return helperRandomDataCount(CANONICAL_START, CANONICAL_END, int64(CANONICAL_COUNT))
}

const BTRDB_LOW int64 = -(16 << 56)
const BTRDB_HIGH int64 = (48 << 56)

func helperStatIsNaN(sp *btrdb.StatPoint) bool {
    return math.IsNaN(sp.Min) && math.IsNaN(sp.Mean) && math.IsNaN(sp.Max)
}

func helperVersion(t *testing.T, ctx context.Context, s *btrdb.Stream) uint64 {
    v, err := s.Version(ctx)
    if err != nil {
        t.Fatalf("version error: %v", err)
    }
    return v
}

/* Tests */

// What happens if you call Nearest on an empty stream?
func TestNearestEmpty(t *testing.T) {
    ctx := context.Background()
    db := helperConnect(t, ctx)
	stream := helperCreateDefaultStream(t, ctx, db, nil, nil)
    _, _, err := stream.Nearest(ctx, 0, 0, false)
    if err == nil || btrdb.ToCodedError(err).Code != bte.NoSuchPoint {
        t.Fatalf("Expected \"no such point\"; got %v", err)
    }
}

// What if there are no more points to the right?
func TestNearestForwardNoPoint(t *testing.T) {
    ctx := context.Background()
    db := helperConnect(t, ctx)
	stream := helperCreateDefaultStream(t, ctx, db, nil, nil)
    helperInsert(t, ctx, stream, helperCanonicalData())
    _, _, err := stream.Nearest(ctx, CANONICAL_END + 1, 0, false)
    if err == nil || btrdb.ToCodedError(err).Code != bte.NoSuchPoint {
        t.Fatalf("Expected \"no such point\"; got %v", err)
    }
}

// Check if forward nearest point queries are really inclusive
func TestNearestForwardInclusive(t *testing.T) {
    ctx := context.Background()
    db := helperConnect(t, ctx)
	stream := helperCreateDefaultStream(t, ctx, db, nil, nil)
    data := helperCanonicalData()
    helperInsert(t, ctx, stream, data)
    rv, _, err := stream.Nearest(ctx, CANONICAL_FINAL, 0, false)
    if err != nil {
        t.Fatalf("Unexpected nearest point error: %v", err)
    }
    if rv != data[len(data) - 1] {
        t.Fatal("Wrong result")
    }
    _, _, err = stream.Nearest(ctx, CANONICAL_FINAL + 1, 0, false)
    if err == nil || btrdb.ToCodedError(err).Code != bte.NoSuchPoint {
        t.Fatalf("Expected \"no such point\"; got %v", err)
    }
}

// What if there are no more points to the left?
func TestNearestBackwardNoPoint(t *testing.T) {
    ctx := context.Background()
    db := helperConnect(t, ctx)
	stream := helperCreateDefaultStream(t, ctx, db, nil, nil)
    helperInsert(t, ctx, stream, helperCanonicalData())
    _, _, err := stream.Nearest(ctx, CANONICAL_START - 1, 0, true)
    if err == nil || btrdb.ToCodedError(err).Code != bte.NoSuchPoint {
        t.Fatalf("Expected \"no such point\"; got %v", err)
    }
}

// Check if backward nearest point queries are really exclusive
func TestNearestBackwardExclusive(t *testing.T) {
    ctx := context.Background()
    db := helperConnect(t, ctx)
	stream := helperCreateDefaultStream(t, ctx, db, nil, nil)
    data := helperCanonicalData()
    helperInsert(t, ctx, stream, data)
    _, _, err := stream.Nearest(ctx, CANONICAL_START, 0, true)
    if err == nil || btrdb.ToCodedError(err).Code != bte.NoSuchPoint {
        t.Fatalf("Expected \"no such point\"; got %v", err)
    }
    rv, _, err := stream.Nearest(ctx, CANONICAL_START + 1, 0, true)
    if err != nil {
        t.Fatalf("Unexpected nearest point error: %v", err)
    }
    if rv != data[0] {
        t.Fatal("Wrong result")
    }
}

// Check if the insert range is really inclusive of the earliest time
func TestEarliestInclusive(t *testing.T) {
    ctx := context.Background()
    db := helperConnect(t, ctx)
	stream := helperCreateDefaultStream(t, ctx, db, nil, nil)
    rp := btrdb.RawPoint{Time: BTRDB_LOW, Value: rand.NormFloat64()}
    helperInsert(t, ctx, stream, []btrdb.RawPoint{rp})
    rv, _, err := stream.Nearest(ctx, BTRDB_LOW, 0, false)
    if err != nil {
        t.Fatalf("Could not find lowest point: %v", err)
    }
    if rv != rp {
        t.Fatal("Lowest point returned incorrectly")
    }
}

// Check if a point before lowest valid time is handled correctly
func TestInsertBeforeRange(t *testing.T) {
    ctx := context.Background()
    db := helperConnect(t, ctx)
	stream := helperCreateDefaultStream(t, ctx, db, nil, nil)
    err := stream.Insert(ctx, []btrdb.RawPoint{btrdb.RawPoint{Time: BTRDB_LOW - 1, Value: rand.NormFloat64()}})
    if err == nil || btrdb.ToCodedError(err).Code != bte.InvalidTimeRange {
        t.Fatalf("Expected \"invalid time range\" error: got %v", err)
    }
}

// Check if the insert range is really exclusive of the latest time
func TestLatestExclusive(t *testing.T) {
    ctx := context.Background()
    db := helperConnect(t, ctx)
	stream := helperCreateDefaultStream(t, ctx, db, nil, nil)
    err := stream.Insert(ctx, []btrdb.RawPoint{btrdb.RawPoint{Time: BTRDB_HIGH, Value: rand.NormFloat64()}})
    if err == nil || btrdb.ToCodedError(err).Code != bte.InvalidTimeRange {
        t.Fatalf("Expected \"invalid time range\" error: got %v", err)
    }
}

// Check if a point at the highest valid time is handled correctly
func TestHighestValid(t *testing.T) {
    ctx := context.Background()
    db := helperConnect(t, ctx)
	stream := helperCreateDefaultStream(t, ctx, db, nil, nil)
    rp := btrdb.RawPoint{Time: BTRDB_HIGH - 1, Value: rand.NormFloat64()}
    helperInsert(t, ctx, stream, []btrdb.RawPoint{rp})
    rv, _, err := stream.Nearest(ctx, BTRDB_HIGH, 0, true)
    if err != nil {
        t.Fatalf("Could not find highest point: %v", err)
    }
    if rv != rp {
        t.Fatal("Highest point returned incorrectly")
    }
}

/* Does the largest possible query work? */
func TestQueryFullTimeRange(t *testing.T) {
    ctx := context.Background()
    db := helperConnect(t, ctx)
	stream := helperCreateDefaultStream(t, ctx, db, nil, nil)
    data := helperCanonicalData()
    helperInsert(t, ctx, stream, data)
    pts, _ := helperRawQuery(t, ctx, stream, CANONICAL_START, CANONICAL_END, 0)
    if len(data) != len(pts) {
        t.Fatalf("Missing or extra points in queried dataset (inserted %v, got %v)", len(data), len(pts))
    }
    for i, rp := range data {
        if rp != pts[i] {
            t.Fatal("Inserted and queried datasets do not match")
        }
    }
}

/* Check if a query in an invalid time range is handled correctly. */
func TestQueryInvalidTimeRange(t *testing.T) {
    ctx := context.Background()
    db := helperConnect(t, ctx)
	stream := helperCreateDefaultStream(t, ctx, db, nil, nil)
    data := helperCanonicalData()
    helperInsert(t, ctx, stream, data)
    _, _, errc := stream.RawValues(ctx, CANONICAL_START - 1, CANONICAL_END + 1, 0)
    err := <-errc
    if err == nil || btrdb.ToCodedError(err).Code != bte.InvalidTimeRange {
        t.Fatalf("Expected \"invalid time range\" error: got %v", err)
    }
}

func TestNaN(t *testing.T) {
    nan1 := math.Float64frombits(0x7FFbadc0ffee7ea5)
    nan2 := math.Float64frombits(0x7FF5dbb0554c0010)
    nan3 := math.Float64frombits(0xFFFbabb1edbee71e)
    nan4 := math.Float64frombits(0xFFF501aceca571e5)
    times := []int64{0, 1000, 2000, 3000, 4000, 5000, 6000, 7000}
    values := []float64{nan1, nan2, nan3, rand.NormFloat64(), rand.NormFloat64(), nan4, rand.NormFloat64(), rand.NormFloat64()}

    ctx := context.Background()
    db := helperConnect(t, ctx)
	stream := helperCreateDefaultStream(t, ctx, db, nil, nil)
    helperInsertTV(t, ctx, stream, times, values)
    pts, _ := helperRawQuery(t, ctx, stream, times[0], times[len(times) - 1] + 1, 0)
    if len(times) != len(pts) {
        t.Fatalf("Missing or extra raw points in queried dataset (expected %v, got %v)", len(times), len(pts))
    }
    for i, rp := range pts {
        if rp.Time != times[i] || math.Float64bits(rp.Value) != math.Float64bits(values[i]) {
            t.Fatal("Inserted and queried datasets do not match")
        }
    }
    spts, _ := helperWindowQuery(t, ctx, stream, 0, 10000, 2000, 0, 0)
    if len(spts) != (len(times) / 2) {
        t.Fatalf("Missing or extra statistical points in queried dataset (expected %v, got %v)", len(times) / 2, len(spts))
    }
    for i, sp := range spts {
        if sp.Time != times[2 * i] {
            t.Fatal("Queried statistical point has unexpected time or count (expected t=%v c=%v, got t=%v c=%v)", times[2 * i], 2, sp.Time, sp.Count)
        }
    }
    if !helperStatIsNaN(&spts[0]) || !helperStatIsNaN(&spts[1]) || !helperStatIsNaN(&spts[2]) {
        t.Fatal("Queried statistical points are not NaN as expected")
    }
    if spts[3].Min != math.Min(values[6], values[7]) || spts[3].Mean != (values[6] + values[7]) / 2 || spts[3].Max != math.Max(values[6], values[7]) {
        t.Fatal("Queried statistical point does not have expected values")
    }
}

func TestInf(t *testing.T) {
    inf1 := math.Inf(1)
    inf2 := math.Inf(-1)
    times := []int64{0, 1000, 2000, 3000, 4000, 5000, 6000, 7000}
    values := []float64{inf1, inf2, inf1, rand.NormFloat64(), rand.NormFloat64(), inf2, rand.NormFloat64(), rand.NormFloat64()}

    ctx := context.Background()
    db := helperConnect(t, ctx)
	stream := helperCreateDefaultStream(t, ctx, db, nil, nil)
    helperInsertTV(t, ctx, stream, times, values)
    pts, _ := helperRawQuery(t, ctx, stream, times[0], times[len(times) - 1] + 1, 0)
    if len(times) != len(pts) {
        t.Fatalf("Missing or extra raw points in queried dataset (expected %v, got %v)", len(times), len(pts))
    }
    for i, rp := range pts {
        if rp.Time != times[i] || math.Float64bits(rp.Value) != math.Float64bits(values[i]) {
            t.Fatal("Inserted and queried datasets do not match")
        }
    }
    spts, _ := helperWindowQuery(t, ctx, stream, 0, 10000, 2000, 0, 0)
    if len(spts) != (len(times) / 2) {
        t.Fatalf("Missing or extra statistical points in queried dataset (expected %v, got %v)", len(times) / 2, len(spts))
    }
    for i, sp := range spts {
        if sp.Time != times[2 * i] || sp.Count != 2 {
            t.Fatal("Queried statistical point has unexpected time or count (expected t=%v c=%v, got t=%v c=%v)", times[2 * i], 2, sp.Time, sp.Count)
        }
    }
    if !math.IsInf(spts[0].Min, -1) || !math.IsNaN(spts[0].Mean) || !math.IsInf(spts[0].Max, 1) {
        t.Fatal("Queried statistical point is not (-Inf, NaN, +Inf) as expected")
    }
    if spts[1].Min != values[3] || !math.IsInf(spts[1].Mean, 1) || !math.IsInf(spts[1].Max, 1) {
        t.Fatalf("Queried statistical point is not (%f, +Inf, +Inf) as expected", values[3])
    }
    if !math.IsInf(spts[2].Min, -1) || !math.IsInf(spts[2].Mean, -1) || spts[2].Max != values[4] {
        t.Fatalf("Queried statistical point is not (-Inf, -Inf, %f) as expected", values[4])
    }
    if spts[3].Min != math.Min(values[6], values[7]) || spts[3].Mean != (values[6] + values[7]) / 2 || spts[3].Max != math.Max(values[6], values[7]) {
        t.Fatal("Queried statistical point does not have expected values")
    }
}

func TestSpecialValues(t *testing.T) {
    highest := math.Float64frombits(0x7FEFFFFFFFFFFFFF)
    lowest := math.Float64frombits(0x7FEFFFFFFFFFFFFF)
    smallestpos := math.Float64frombits(0x0000000000000001)
    zeropos := math.Float64frombits(0x0000000000000000)
    zeroneg := math.Float64frombits(0x8000000000000000)
    times := []int64{0, 1000, 2000, 3000, 4000, 5000, 6000, 7000}
    values := []float64{highest, highest, lowest, lowest, smallestpos, zeroneg, zeropos, smallestpos}

    ctx := context.Background()
    db := helperConnect(t, ctx)
	stream := helperCreateDefaultStream(t, ctx, db, nil, nil)
    helperInsertTV(t, ctx, stream, times, values)
    pts, _ := helperRawQuery(t, ctx, stream, times[0], times[len(times) - 1] + 1, 0)
    if len(times) != len(pts) {
        t.Fatalf("Missing or extra raw points in queried dataset (expected %v, got %v)", len(times), len(pts))
    }
    for i, rp := range pts {
        if rp.Time != times[i] || math.Float64bits(rp.Value) != math.Float64bits(values[i]) {
            t.Fatal("Inserted and queried datasets do not match")
        }
    }
    spts, _ := helperWindowQuery(t, ctx, stream, 0, 10000, 2000, 0, 0)
    if len(spts) != (len(times) / 2) {
        t.Log(spts)
        t.Fatalf("Missing or extra statistical points in queried dataset (expected %v, got %v)", len(times) / 2, len(spts))
    }
    for i, sp := range spts {
        if sp.Time != times[2 * i] || sp.Count != 2 || sp.Min != math.Min(values[2 * i], values[2 * i + 1]) || sp.Max != math.Max(values[2 * i], values[2 * i + 1]) {
            t.Fatal("Queried statistical point has unexpected time or count (expected t=%v c=%v min=%v max=%v, got t=%v c=%v min=%v max=%v)", times[2 * i], 2, math.Min(values[2 * i], values[2 * i + 1]), math.Max(values[2 * i], values[2 * i + 1]), sp.Time, sp.Count, sp.Min, sp.Max)
        }
    }
    if !(math.IsInf(spts[0].Mean, 1) || spts[0].Mean == highest) {
        t.Errorf("Mean of (highest, highest) must be +Inf or highest: got %f", spts[0].Mean)
    }
    if !(math.IsInf(spts[1].Mean, -1) || spts[1].Mean == lowest) {
        t.Errorf("Mean of (lowest, lowest) must be -Inf or lowest: got %f", spts[1].Mean)
    }
    if spts[2].Mean != values[4] / 2.0 {
        t.Errorf("Mean of (val, -0.0): expected %f, got %f", values[4] / 2.0, spts[2].Mean)
    }
    if spts[3].Mean != values[7] / 2.0 {
        t.Errorf("Mean of (0.0, val): expected %f, got %f", values[7] / 2.0, spts[3].Mean)
    }
}

func TestWindowBoundaryRounding1(t *testing.T) {
    ctx := context.Background()
    db := helperConnect(t, ctx)
	stream := helperCreateDefaultStream(t, ctx, db, nil, nil)
    data := helperRandomData(10000, 20000, 10)
    helperInsert(t, ctx, stream, data)
    spts, _ := helperWindowQuery(t, ctx, stream, 11136, 11647, 64, 0, 0)
    if len(spts) != 7 {
        t.Log(spts)
        t.Fatalf("Expected 7 points: got %d", len(spts))
    }
    for i, sp := range spts {
        if sp.Time != 11136 + (int64(i) * 64) {
            t.Errorf("Queried point %d expected at %v: got %v", i, 11136 + (i * 64), sp.Time)
        }
    }
}

func TestWindowBoundaryRounding2(t *testing.T) {
    ctx := context.Background()
    db := helperConnect(t, ctx)
	stream := helperCreateDefaultStream(t, ctx, db, nil, nil)
    data := helperRandomData(10000, 20000, 10)
    helperInsert(t, ctx, stream, data)
    spts, _ := helperWindowQuery(t, ctx, stream, 11136, 11584, 64, 0, 0)
    if len(spts) != 7 {
        t.Log(spts)
        t.Fatalf("Expected 7 points: got %d", len(spts))
    }
    for i, sp := range spts {
        if sp.Time != 11136 + (int64(i) * 64) {
            t.Errorf("Queried point %d expected at %v: got %v", i, 11136 + (i * 64), sp.Time)
        }
    }
}

func TestStatisticalBoundaryRounding1(t *testing.T) {
    ctx := context.Background()
    db := helperConnect(t, ctx)
	stream := helperCreateDefaultStream(t, ctx, db, nil, nil)
    data := helperRandomData(10000, 20000, 10)
    helperInsert(t, ctx, stream, data)
    spts, _ := helperStatisticalQuery(t, ctx, stream, 11136, 11647, 6, 0)
    if len(spts) != 7 {
        t.Log(spts)
        t.Fatalf("Expected 7 points: got %d", len(spts))
    }
    for i, sp := range spts {
        if sp.Time != 11136 + (int64(i) * 64) {
            t.Errorf("Queried point %d expected at %v: got %v", i, 11136 + (i * 64), sp.Time)
        }
    }
}

func TestStatisticalBoundaryRounding2(t *testing.T) {
    ctx := context.Background()
    db := helperConnect(t, ctx)
	stream := helperCreateDefaultStream(t, ctx, db, nil, nil)
    data := helperRandomData(10000, 20000, 10)
    helperInsert(t, ctx, stream, data)
    spts, _ := helperStatisticalQuery(t, ctx, stream, 11199, 11584, 6, 0)
    if len(spts) != 7 {
        t.Log(spts)
        t.Fatalf("Expected 7 points: got %d", len(spts))
    }
    for i, sp := range spts {
        if sp.Time != 11136 + (int64(i) * 64) {
            t.Errorf("Queried point %d expected at %v: got %v", i, 11136 + (i * 64), sp.Time)
        }
    }
}

func TestRawBoundaryRounding(t *testing.T) {
    ctx := context.Background()
    db := helperConnect(t, ctx)
	stream := helperCreateDefaultStream(t, ctx, db, nil, nil)
    data := helperRandomData(10000, 20000, 10)
    helperInsert(t, ctx, stream, data)
    rpts, _ := helperRawQuery(t, ctx, stream, 10000, 19990, 0)
    if len(rpts) != 999 {
        t.Fatalf("Expected 999 points: got %d", len(rpts))
    }
    for i, rp := range rpts {
        if rp.Time != 10000 + (int64(i) * 10) {
            t.Errorf("Queried point %d expected at %v: got %v", i, 10000 + (int64(i) * 10), rp.Time)
        }
    }
}

func TestWindowSmallRange1(t *testing.T) {
    ctx := context.Background()
    db := helperConnect(t, ctx)
	stream := helperCreateDefaultStream(t, ctx, db, nil, nil)
    data := helperRandomData(10000, 20000, 10)
    helperInsert(t, ctx, stream, data)
    spts, _ := helperWindowQuery(t, ctx, stream, 11137, 11200, 64, 0, 0)
    if len(spts) != 0 {
        t.Log(spts)
        t.Fatalf("Expected 0 points: got %d", len(spts))
    }
}

func TestWindowSmallRange2(t *testing.T) {
    ctx := context.Background()
    db := helperConnect(t, ctx)
	stream := helperCreateDefaultStream(t, ctx, db, nil, nil)
    data := helperRandomData(10000, 20000, 10)
    helperInsert(t, ctx, stream, data)
    spts, _ := helperWindowQuery(t, ctx, stream, 11137, 11201, 64, 0, 0)
    if len(spts) != 1 {
        t.Log(spts)
        t.Fatalf("Expected 1 point: got %d", len(spts))
    }
    if spts[0].Time != 11137 {
        t.Fatalf("Point is at incorrect time: expected 11137, got %v", spts[0].Time)
    }
}

func TestStatisticalSmallRange1(t *testing.T) {
    ctx := context.Background()
    db := helperConnect(t, ctx)
	stream := helperCreateDefaultStream(t, ctx, db, nil, nil)
    data := helperRandomData(10000, 20000, 10)
    helperInsert(t, ctx, stream, data)
    spts, _ := helperStatisticalQuery(t, ctx, stream, 11136, 11199, 6, 0)
    if len(spts) != 0 {
        t.Log(spts)
        t.Fatalf("Expected 0 points: got %d", len(spts))
    }
}

func TestStatisticalSmallRange2(t *testing.T) {
    ctx := context.Background()
    db := helperConnect(t, ctx)
	stream := helperCreateDefaultStream(t, ctx, db, nil, nil)
    data := helperRandomData(10000, 20000, 10)
    helperInsert(t, ctx, stream, data)
    spts, _ := helperStatisticalQuery(t, ctx, stream, 11199, 11200, 6, 0)
    if len(spts) != 1 {
        t.Log(spts)
        t.Fatalf("Expected 1 point: got %d", len(spts))
    }
    if spts[0].Time != 11136 {
        t.Fatalf("Point is at incorrect time: expected 11136, got %v", spts[0].Time)
    }
}

func TestRawSmallRange1(t *testing.T) {
    ctx := context.Background()
    db := helperConnect(t, ctx)
	stream := helperCreateDefaultStream(t, ctx, db, nil, nil)
    data := helperRandomData(10000, 20000, 10)
    helperInsert(t, ctx, stream, data)
    rpts, _ := helperRawQuery(t, ctx, stream, 12001, 12010, 0)
    if len(rpts) != 0 {
        t.Log(rpts)
        t.Fatalf("Expected 0 points: got %d", len(rpts))
    }
}

func TestRawSmallRange2(t *testing.T) {
    ctx := context.Background()
    db := helperConnect(t, ctx)
	stream := helperCreateDefaultStream(t, ctx, db, nil, nil)
    data := helperRandomData(10000, 20000, 10)
    helperInsert(t, ctx, stream, data)
    rpts, _ := helperRawQuery(t, ctx, stream, 12000, 12010, 0)
    if len(rpts) != 1 {
        t.Log(rpts)
        t.Fatalf("Expected 1 point: got %d", len(rpts))
    }
    if rpts[0].Time != 12000 {
        t.Fatalf("Point is at incorrect time: expected 12000, got %v", rpts[0].Time)
    }
}

func TestWindowNegativeRange(t *testing.T) {
    ctx := context.Background()
    db := helperConnect(t, ctx)
	stream := helperCreateDefaultStream(t, ctx, db, nil, nil)
    helperInsert(t, ctx, stream, helperCanonicalData())
    _, _, errc := stream.Windows(ctx, CANONICAL_START + 100, CANONICAL_START + 50, 10, 0, 0)
    err := <-errc
    if err == nil || btrdb.ToCodedError(err).Code != bte.InvalidTimeRange {
        t.Fatalf("Expected \"invalid time range\"; got %v", err)
    }
}

func TestWindowInvalidDepth(t *testing.T) {
    ctx := context.Background()
    db := helperConnect(t, ctx)
	stream := helperCreateDefaultStream(t, ctx, db, nil, nil)
    helperInsert(t, ctx, stream, helperCanonicalData())
    _, _, errc := stream.Windows(ctx, CANONICAL_START + 50, CANONICAL_START + 100, 10, 64, 0)
    err := <-errc
    if err == nil || btrdb.ToCodedError(err).Code != bte.InvalidPointWidth {
        t.Fatalf("Expected \"bad point width\"; got %v", err)
    }
}

func TestWindowInvalidVersion(t *testing.T) {
    ctx := context.Background()
    db := helperConnect(t, ctx)
	stream := helperCreateDefaultStream(t, ctx, db, nil, nil)
    helperInsert(t, ctx, stream, helperCanonicalData())
    ver := helperVersion(t, ctx, stream)
    _, _, errc := stream.Windows(ctx, CANONICAL_START + 50, CANONICAL_START + 100, 10, 61, ver + 1)
    err := <-errc
    if err == nil || btrdb.ToCodedError(err).Code != bte.NoSuchStream {
        t.Fatalf("Expected \"no such stream\"; got %v", err)
    }
}

func TestStatisticalNegativeRange(t *testing.T) {
    ctx := context.Background()
    db := helperConnect(t, ctx)
	stream := helperCreateDefaultStream(t, ctx, db, nil, nil)
    helperInsert(t, ctx, stream, helperCanonicalData())
    _, _, errc := stream.AlignedWindows(ctx, CANONICAL_START + 100, CANONICAL_START + 50, 3, 0)
    err := <-errc
    if err == nil || btrdb.ToCodedError(err).Code != bte.InvalidTimeRange {
        t.Fatalf("Expected \"invalid time range\"; got %v", err)
    }
}

func TestStatisticalInvalidPointWidth(t *testing.T) {
    ctx := context.Background()
    db := helperConnect(t, ctx)
	stream := helperCreateDefaultStream(t, ctx, db, nil, nil)
    helperInsert(t, ctx, stream, helperCanonicalData())
    _, _, errc := stream.AlignedWindows(ctx, CANONICAL_START + 50, CANONICAL_START + 100, 64, 0)
    err := <-errc
    if err == nil || btrdb.ToCodedError(err).Code != bte.InvalidPointWidth {
        t.Fatalf("Expected \"bad point width\"; got %v", err)
    }
}

func TestStatisticalInvalidVersion(t *testing.T) {
    ctx := context.Background()
    db := helperConnect(t, ctx)
	stream := helperCreateDefaultStream(t, ctx, db, nil, nil)
    helperInsert(t, ctx, stream, helperCanonicalData())
    ver := helperVersion(t, ctx, stream)
    _, _, errc := stream.AlignedWindows(ctx, CANONICAL_START + 50, CANONICAL_START + 100, 61, ver + 1)
    err := <-errc
    if err == nil || btrdb.ToCodedError(err).Code != bte.NoSuchStream {
        t.Fatalf("Expected \"no such stream\"; got %v", err)
    }
}

func TestRawNegativeRange(t *testing.T) {
    ctx := context.Background()
    db := helperConnect(t, ctx)
	stream := helperCreateDefaultStream(t, ctx, db, nil, nil)
    helperInsert(t, ctx, stream, helperCanonicalData())
    _, _, errc := stream.RawValues(ctx, CANONICAL_START + 100, CANONICAL_START + 50, 0)
    err := <-errc
    if err == nil || btrdb.ToCodedError(err).Code != bte.InvalidTimeRange {
        t.Fatalf("Expected \"invalid time range\"; got %v", err)
    }
}

func TestRawInvalidVersion(t *testing.T) {
    ctx := context.Background()
    db := helperConnect(t, ctx)
	stream := helperCreateDefaultStream(t, ctx, db, nil, nil)
    helperInsert(t, ctx, stream, helperCanonicalData())
    ver := helperVersion(t, ctx, stream)
    _, _, errc := stream.RawValues(ctx, CANONICAL_START + 50, CANONICAL_START + 100, ver + 1)
    err := <-errc
    if err == nil || btrdb.ToCodedError(err).Code != bte.NoSuchStream {
        t.Fatalf("Expected \"no such stream\"; got %v", err)
    }
}

func TestClosedChannel(t *testing.T) {
    ctx := context.Background()
    db := helperConnect(t, ctx)
	stream := helperCreateDefaultStream(t, ctx, db, nil, nil)
    helperInsert(t, ctx, stream, helperCanonicalData())
    resp, _, _ := stream.RawValues(ctx, CANONICAL_START, CANONICAL_END + 1, 0)
    close(resp)

    // Check if the bindings still work
    helperRawQuery(t, ctx, stream, CANONICAL_START, CANONICAL_END + 1, 0)
}

const BIG_LOW = 0
const BIG_HIGH = 1485470183000000000
const BIG_GAP = 11432156527

func helperOOMGen() []btrdb.RawPoint {
    fmt.Println("Generating data...")
    return helperRandomData(BIG_LOW, BIG_HIGH, BIG_GAP)
}
/* Moving this to separate function helps with garbage collection. */
func helperOOMInsert(t *testing.T, ctx context.Context, s *btrdb.Stream) {
    bigdata := helperOOMGen()
    fmt.Println("Inserting data...")
    helperInsert(t, ctx, s, bigdata)
}

func TestOOM(t *testing.T) {
    if testing.Short() {
        t.Skip()
    }

    ctx := context.Background()
    db := helperConnect(t, ctx)
	stream := helperCreateDefaultStream(t, ctx, db, nil, nil)
    helperOOMInsert(t, ctx, stream)

    fmt.Println("Making queries...")

    const NUM_QUERIES = 2000000

    var chans []chan btrdb.RawPoint = make([]chan btrdb.RawPoint, NUM_QUERIES)
    var errchans []chan error = make([]chan error, NUM_QUERIES)

    for i := 0; i < NUM_QUERIES; i++ {
        c, _, ec := stream.RawValues(ctx, BIG_LOW, BIG_HIGH + 1, 0)
        chans[i] = c
        errchans[i] = ec
    }

    fmt.Println("Waiting for 10 seconds...")
    time.Sleep(10 * time.Second)

    fmt.Println("Checking if an error happened...")
    for j, ec := range errchans {
        select {
        case err := <-ec:
            if err != nil {
                t.Fatalf("Error in query: %v (first resp is %v)", err, <-chans[j])
            }
        default:
        }
    }

    fmt.Println("Checking if the database is still responsive...")
    db2 := helperConnect(t, ctx)
    stream2 := helperCreateDefaultStream(t, ctx, db2, nil, nil)
    helperInsert(t, ctx, stream2, helperCanonicalData())
}

func TestContextCancel(t *testing.T) {
    if testing.Short() {
        t.Skip()
    }

    ctx, cancelfunc := context.WithCancel(context.Background())
    db := helperConnect(t, ctx)
	stream := helperCreateDefaultStream(t, ctx, db, nil, nil)
    helperOOMInsert(t, ctx, stream)

    fmt.Println("Querying data...")

    c, _, ec := stream.RawValues(ctx, BIG_LOW, BIG_HIGH + 1, 0)
    go func() {
        time.Sleep(time.Second)
        cancelfunc()
    }()

    var count int64 = 0
    for _ = range c {
        count++
    }
    err := <-ec
    if err == nil || btrdb.ToCodedError(err).Code != bte.ContextError {
        t.Errorf("Expected \"context error\"; got %v", err)
    }

    if count != (BIG_HIGH - BIG_LOW) / BIG_GAP {
        t.Logf("Got fewer points than inserted, as expected (inserted %v points; got %v)", (BIG_HIGH - BIG_LOW) / BIG_GAP, count)
    }
}

func TestRawCorrect(t *testing.T) {
    ctx := context.Background()
    db := helperConnect(t, ctx)
	stream := helperCreateDefaultStream(t, ctx, db, nil, nil)
    data := helperCanonicalData()
    fmt.Println("Inserting...")
    helperInsert(t, ctx, stream, data)
    fmt.Println("Querying...")
    rpts, _ := helperRawQuery(t, ctx, stream, CANONICAL_START, CANONICAL_END + 1, 0)
    fmt.Println("Verifying...")
    if len(rpts) != len(data) {
        t.Fatalf("Did not receive the same number of points as were inserted: inserted %v, but received %v", len(data), len(rpts))
    }
    for i, rp := range rpts {
        if rp != data[i] {
            t.Fatalf("Received point at index %d does not match inserted point (inserted %v but received %v)", i, data[i], rp)
        }
    }
}

func helperFloatEquals(x float64, y float64) bool {
	return math.Abs(x - y) < 1e-14 * math.Max(math.Abs(x), math.Abs(y))
}

func TestStatisticalCorrect(t *testing.T) {
    ctx := context.Background()
    db := helperConnect(t, ctx)
	stream := helperCreateDefaultStream(t, ctx, db, nil, nil)
    data := helperCanonicalData()
    fmt.Println("Inserting...")
    helperInsert(t, ctx, stream, data)
    var pwe uint8
    for pwe = 48; pwe <= 52; pwe++ {
        fmt.Printf("Querying pwe=%v...\n", pwe)
        spts, _ := helperStatisticalQuery(t, ctx, stream, CANONICAL_START, CANONICAL_END + (int64(1) << pwe), pwe, 0)
        fmt.Printf("Verifying pwe=%v [len=%d]...\n", pwe, len(spts))
        dataidx := 0
        for j, sp := range spts {
            if (sp.Time & ((int64(1) << pwe) - 1)) != 0 {
                t.Fatalf("Returned statistical point is at time %v, which is not aligned", sp.Time)
            }
            if sp.Count == 0 {
                t.Fatalf("Returned statistical point at index %v has Count == 0", j)
            }
            endtime := sp.Time + (int64(1) << pwe)
            count := uint64(0)
            min := math.Inf(1)
            max := math.Inf(-1)
            sum := 0.0
            for ; dataidx != len(data) && data[dataidx].Time < endtime; dataidx++ {
                rp := &data[dataidx]
                if rp.Time < sp.Time {
                    t.Fatalf("Returned statistical points skip some inserted points (point at %v but skips to %v)", rp.Time, sp.Time)
                }
                min = math.Min(min, rp.Value)
                sum += rp.Value
                max = math.Max(max, rp.Value)
                count++
            }
            mean := sum / float64(count)
            if min != sp.Min || !helperFloatEquals(mean, sp.Mean) || max != sp.Max || count != sp.Count {
                t.Fatalf("Returned statistical point at index %d doesn't match expected (got %v but expected {%v %v %v %v %v})", j, sp, sp.Time, min, mean, max, count)
            }
        }
        if dataidx != len(data) {
            t.Fatalf("Unaccounted raw points lie after returned statistical points: %v", data[dataidx:])
        }
    }
}

// We should also test varying the depth, but I don't know exactly how BTrDB
// does the approximation for nonzero depth, so I don't know how to test for
// correctness there.
func TestWindowCorrect(t *testing.T) {
    ctx := context.Background()
    db := helperConnect(t, ctx)
	stream := helperCreateDefaultStream(t, ctx, db, nil, nil)
    data := helperCanonicalData()
    fmt.Println("Inserting...")
    helperInsert(t, ctx, stream, data)
    var width uint64
    var widths = []uint64{99999999999990, 100000000000000, 199999999999999, 200000000000000, 220000000000000, 3184713375796179, 76923076923076915, 99999999999999, 1000000000000000000}
    for _, width = range widths {
        fmt.Printf("Querying width=%v...\n", width)
        spts, _ := helperWindowQuery(t, ctx, stream, CANONICAL_START, CANONICAL_END + int64(width), width, 0, 0)
        fmt.Printf("Verifying width=%v [len=%d]...\n", width, len(spts))
        dataidx := 0
        for j, sp := range spts {
            if (uint64(sp.Time - CANONICAL_START) % width) != 0 {
                t.Fatalf("Returned statistical point is at time %v, which is not aligned", sp.Time)
            }
            if sp.Count == 0 {
                t.Fatalf("Returned statistical point at index %v has Count == 0", j)
            }
            endtime := sp.Time + int64(width)
            count := uint64(0)
            min := math.Inf(1)
            max := math.Inf(-1)
            sum := 0.0
            for ; dataidx != len(data) && data[dataidx].Time < endtime; dataidx++ {
                rp := &data[dataidx]
                if rp.Time < sp.Time {
                    t.Fatalf("Returned statistical points skip some inserted points (point at %v but skips to %v)", rp.Time, sp.Time)
                }
                min = math.Min(min, rp.Value)
                sum += rp.Value
                max = math.Max(max, rp.Value)
                count++
            }
            mean := sum / float64(count)
            if min != sp.Min || !helperFloatEquals(mean, sp.Mean) || max != sp.Max || count != sp.Count {
                t.Fatalf("Returned statistical point at index %d doesn't match expected (got %v but expected {%v %v %v %v %v})", j, sp, sp.Time, min, mean, max, count)
            }
        }
        if dataidx != len(data) {
            t.Fatalf("Unaccounted raw points lie after returned statistical points: %v", data[dataidx:])
        }
    }
}
