package tests

import (
	"context"
	//"fmt"
	"testing"

	//"github.com/davecgh/go-spew/spew"
	"github.com/BTrDB/btrdb"
)

type Queryable interface {
	DoQuery(*btrdb.Stream, int64, int64, int64) ([]btrdb.StatPoint, uint64, int64)
	GetContext() context.Context
	MakeStatPoints([]btrdb.RawPoint, int64, int64, int64) []btrdb.StatPoint
}

type AlignedWindowsQuery struct {
	t   *testing.T
	ctx context.Context
}

func (awq AlignedWindowsQuery) GetContext() context.Context {
	return awq.ctx
}

func (awq AlignedWindowsQuery) DoQuery(s *btrdb.Stream, start int64, end int64, count int64) ([]btrdb.StatPoint, uint64, int64) {
	pwe := uint8(52)
	width := int64(1) << pwe
	result, version := helperStatisticalQuery(awq.t, awq.ctx, s, start, end+width, pwe, 0)
	return result, version, width
}

func (awq AlignedWindowsQuery) MakeStatPoints(points []btrdb.RawPoint, start int64, end, width int64) []btrdb.StatPoint {
	return helperMakeStatPoints(points, start, end, width, true)
}

type WindowsQuery struct {
	t   *testing.T
	ctx context.Context
}

func (wq WindowsQuery) GetContext() context.Context {
	return wq.ctx
}

func (wq WindowsQuery) DoQuery(s *btrdb.Stream, start int64, end int64, count int64) ([]btrdb.StatPoint, uint64, int64) {
	width := int64(end-start) / 4
	result, version := helperWindowQuery(wq.t, wq.ctx, s, start, end+width, uint64(width), 0, 0)
	return result, version, width
}

func (wq WindowsQuery) MakeStatPoints(points []btrdb.RawPoint, start int64, end int64, width int64) []btrdb.StatPoint {
	return helperMakeStatPoints(points, start, end, width, false)
}

func RunTestQueryWithHoles(t *testing.T, q Queryable, scount int) {
	ctx := q.GetContext()
	db := helperConnect(t, ctx)
	stream := helperCreateDefaultStream(t, ctx, db, btrdb.OptKV("name", "n"), nil)
	start := int64(1519088910000000000) // Random unix datetime
	midEnd := start + 1000000
	midStart := midEnd + 100000
	finalEnd := midStart + 1000000
	count := int64(scount)
	firstData := helperRandomDataCount(start, midEnd, count)
	helperInsert(t, ctx, stream, firstData)
	secondData := helperRandomDataCount(midStart, finalEnd, count)
	helperInsert(t, ctx, stream, secondData)
	spts, _, width := q.DoQuery(stream, start, finalEnd, count*2)
	allData := make([]btrdb.RawPoint, 0)
	allData = append(allData, firstData...)
	allData = append(allData, secondData...)
	calculated := q.MakeStatPoints(allData, start, finalEnd, int64(width))
	err := helperCheckStatisticalEqual(calculated, spts)
	if err != nil {
		t.Fatalf("Queried data was invalid: %v", err)
	}
}

func RunTestQueryFlushing(t *testing.T, q Queryable, scount int) {
	ctx := q.GetContext()
	db := helperConnect(t, ctx)
	stream := helperCreateDefaultStream(t, ctx, db, btrdb.OptKV("name", "n"), nil)
	start := int64(1519088910000000000) // Random unix datetime
	end := start + 1000000
	count := int64(scount)
	data := helperRandomDataCount(start, end, count)
	err := stream.Insert(ctx, data)
	if err != nil {
		t.Fatalf("Error from insert %v", err)
	}
	unflushed, _, width := q.DoQuery(stream, start, end, count)
	err = stream.Flush(ctx)
	if err != nil {
		t.Fatalf("Error from Flush %v", err)
	}
	if len(unflushed) == 0 {
		t.Fatal("Unflushed query was empty")
	}
	flushed, _, _ := q.DoQuery(stream, start, end, count)
	if len(flushed) == 0 {
		t.Fatal("Flushed query was empty")
	}
	calculated := q.MakeStatPoints(data, start, end, width)
	err = helperCheckStatisticalEqual(unflushed, calculated)
	if err != nil {
		t.Fatalf("Unflushed and calculated queries were not equal: %v", err)
	}
	err = helperCheckStatisticalEqual(flushed, calculated)
	if err != nil {
		t.Fatalf("Flushed and calculated queries were not equal: %v", err)
	}
}

func doWindowsQuery(t *testing.T, ctx context.Context, s *btrdb.Stream, start int64, end int64, count int64) ([]btrdb.StatPoint, uint64, int64) {
	width := int64(end-start) / 15
	result, version := helperWindowQuery(t, ctx, s, start, end+width, uint64(width), 0, 0)
	return result, version, width
}

func doAlignedWindowsQuery(t *testing.T, ctx context.Context, s *btrdb.Stream, start int64, end int64, count int64) ([]btrdb.StatPoint, uint64, int64) {
	pwe := uint8(30)
	width := int64(1) << pwe
	result, version := helperStatisticalQuery(t, ctx, s, start, end+width, pwe, 0)
	return result, version, width
}

func TestWindowsQueryWithHole(t *testing.T) {
	wq := WindowsQuery{t, context.Background()}
	RunTestQueryWithHoles(t, wq, 100000)
}

func TestdWindowsAligneQueryWithHole(t *testing.T) {
	awq := AlignedWindowsQuery{t, context.Background()}
	RunTestQueryWithHoles(t, awq, 100000)
}

func TestWindowsQueryFlushing(t *testing.T) {
	wq := WindowsQuery{t, context.Background()}
	RunTestQueryFlushing(t, wq, 100000)
}

func TestWindowsAlignedQueryFlushing(t *testing.T) {
	awq := AlignedWindowsQuery{t, context.Background()}
	RunTestQueryFlushing(t, awq, 100000)
}

//The database would actually flush an insert of 100k points. To properly test
//unflushed inserts we need to use less points:

func TestWindowsQueryNoInitFlushing(t *testing.T) {
	wq := WindowsQuery{t, context.Background()}
	RunTestQueryFlushing(t, wq, 500)
}

func TestWindowsAlignedQueryNoInitFlushing(t *testing.T) {
	awq := AlignedWindowsQuery{t, context.Background()}
	RunTestQueryFlushing(t, awq, 500)
}
