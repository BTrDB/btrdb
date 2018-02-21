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

func helperCheckStatisticalCorrect(points []btrdb.RawPoint, statPoints []btrdb.StatPoint, queryWidth int64) error {
	pointsIndex := 0
	for _, sp := range statPoints {
		endtime := sp.Time + int64(queryWidth)
		count := uint64(0)
		min := math.Inf(1)
		max := math.Inf(-1)
		sum := 0.0
		fmt.Println(sp.Time)
		for pointsIndex < len(points) && points[pointsIndex].Time < endtime {
			point := &points[pointsIndex]
			if point.Time < sp.Time || point.Time > sp.Time+queryWidth {
				return fmt.Errorf("Point at index %v time %v was out of range %v-%v", pointsIndex, point.Time, sp.Time, sp.Time+queryWidth)
			}
			min = math.Min(min, point.Value)
			sum += point.Value
			max = math.Max(max, point.Value)
			count++
			pointsIndex++
		}
		if min == math.Inf(1) {
			fmt.Printf("index %v sp %v", sp, points[pointsIndex])
		}
		mean := sum / float64(count)
		if min != sp.Min {
			return fmt.Errorf("Calculated min %v did not equal stat point min %v", min, sp.Min)
		}
		if max != sp.Max {
			return fmt.Errorf("Calculated max %v did not equal stat point max %v", max, sp.Max)
		}
		if count != sp.Count {
			return fmt.Errorf("Calculated point count %v did not equal stat point count %v", count, sp.Count)
		}
		if !helperFloatEquals(mean, sp.Mean) {
			return fmt.Errorf("Calculated mean %v did not equal stat point mean %v", mean, sp.Mean)
		}
	}
	return nil
}

func helperCheckStatisticalEqual(a []btrdb.StatPoint, b []btrdb.StatPoint) error {
	if len(a) != len(b) {
		return fmt.Errorf("Stat point lenghts did not match %v %v", len(a), len(b))
	}

	for i := range a {
		if a[i] != b[i] {
			return fmt.Errorf("Stat point %v is not equal to %v", a[i], b[i])
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
