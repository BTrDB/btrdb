package tests

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/pborman/uuid"

	"gopkg.in/BTrDB/btrdb.v4"
)

func helperConnect(t *testing.T, ctx context.Context) *btrdb.BTrDB {
	db, err := btrdb.Connect(ctx, btrdb.EndpointsFromEnv()...)
	if err != nil {
		t.Fatalf("Unexpected connection error: %v", err)
	}
	return db
}

func helperGetCollection(uu uuid.UUID) string {
	return fmt.Sprintf("test.%x", uu[:])
}

func helperCreateStream(t *testing.T, ctx context.Context, db *btrdb.BTrDB, uu uuid.UUID, coll string, tags map[string]string, ann map[string]string) *btrdb.Stream {
	stream, err := db.Create(ctx, uu, coll, tags, ann)
	if err != nil {
		t.Fatalf("create error %v", err)
	}
	return stream
}

func helperCreateDefaultStream(t *testing.T, ctx context.Context, db *btrdb.BTrDB, tags map[string]string, ann map[string]string) *btrdb.Stream {
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

func helperWaitAfterInsert(t *testing.T, ctx context.Context, s *btrdb.Stream) {
	err := s.Flush(ctx)
	if err != nil {
		t.Fatalf("Error from Flush %v", err)
	}
}

func helperInsert(t *testing.T, ctx context.Context, s *btrdb.Stream, data []btrdb.RawPoint) {
	err := s.Insert(ctx, data)
	if err != nil {
		t.Fatalf("Error from Insert %v", err)
	}
	helperWaitAfterInsert(t, ctx, s)
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

func helperFloatEquals(x float64, y float64) bool {
	return math.Abs(x-y) < 1e-10*math.Max(math.Abs(x), math.Abs(y))
}

func helperMakeStatPoints(points []btrdb.RawPoint, start int64, end int64, queryWidth int64, aligned bool) []btrdb.StatPoint {
	end = start + queryWidth*((end-start)/queryWidth)
	numPoints := int((end - start) / queryWidth)
	fmt.Printf("%v - %v / %v = %v\n", end, start, queryWidth, numPoints)
	var statPoints []btrdb.StatPoint
	index := 0
	for index < len(points) {
		offset := index
		var windowStart int64
		if index == 0 {
			windowStart = start
		} else {
			windowStart = points[index].Time
		}
		for offset < len(points) && points[offset].Time-windowStart < queryWidth {
			offset++
		}
		pointsSlice := points[index:offset]
		min := math.Inf(1)
		max := math.Inf(-1)
		sum := 0.0
		for _, point := range pointsSlice {
			min = math.Min(min, point.Value)
			sum += point.Value
			max = math.Max(max, point.Value)
		}
		mean := sum / float64(len(pointsSlice))
		statPoint := btrdb.StatPoint{windowStart, min, mean, max, uint64(len(pointsSlice))}
		statPoints = append(statPoints, statPoint)
		index = offset + 1
	}
	return statPoints
}

func helperCheckStatisticalEqual(firstPoints []btrdb.StatPoint, secondPoints []btrdb.StatPoint) error {
	if len(firstPoints) != len(secondPoints) {
		return fmt.Errorf("Stat point lenghts did not match %v %v", len(firstPoints), len(secondPoints))
	}

	for i := range firstPoints {
		first := firstPoints[i]
		second := secondPoints[i]
		if !helperFloatEquals(first.Min, second.Min) {
			return fmt.Errorf("First min %v did not equal second min %v", first.Min, second.Min)
		}
		if !helperFloatEquals(first.Max, second.Max) {
			return fmt.Errorf("First max %v did not equal second max %v", first.Min, second.Max)
		}
		if first.Count != second.Count {
			return fmt.Errorf("First point count %v did not equal second point %v", first.Count, second.Count)
		}
		if !helperFloatEquals(first.Mean, second.Mean) {
			return fmt.Errorf("First mean %v did not equal second mean %v", first.Mean, second.Mean)
		}
	}

	return nil
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
