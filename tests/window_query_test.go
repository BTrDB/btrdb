package tests

import (
	"context"
	"fmt"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"gopkg.in/BTrDB/btrdb.v4"
)

type QueryFunc func(t *testing.T, ctx context.Context, s *btrdb.Stream, start int64, end int64, count int64) ([]btrdb.StatPoint, uint64, int64)

func RunTestQueryWithHoles(t *testing.T, query QueryFunc, scount int) {
	ctx := context.Background()
	db := helperConnect(t, ctx)
	stream := helperCreateDefaultStream(t, ctx, db, nil, nil)
	start := int64(1519088910) // Random unix datetime
	midEnd := start + 1000000
	midStart := midEnd + 100000
	finalEnd := midStart + 1000000
	count := int64(scount)
	firstData := helperRandomDataCount(start, midEnd, count)
	helperInsert(t, ctx, stream, firstData)
	secondData := helperRandomDataCount(midStart, finalEnd, count)
	helperInsert(t, ctx, stream, secondData)
	spts, _, width := query(t, ctx, stream, start, finalEnd, count*2)
	allData := make([]btrdb.RawPoint, 0)
	allData = append(allData, firstData...)
	allData = append(allData, secondData...)
	err := helperCheckStatisticalCorrect(allData, spts, start, int64(width))
	if err != nil {
		t.Fatalf("Queried data was invalid: %v", err)
	}
}

func RunTestQueryFlushing(t *testing.T, query QueryFunc, scount int) {
	ctx := context.Background()
	db := helperConnect(t, ctx)
	stream := helperCreateDefaultStream(t, ctx, db, nil, nil)
	start := int64(1519088910) // Random unix datetime
	end := start + 1000000
	count := int64(scount)
	data := helperRandomDataCount(start, end, count)
	err := stream.Insert(ctx, data)
	if err != nil {
		t.Fatalf("Error from insert %v", err)
	}
	unflushed, _, width := query(t, ctx, stream, start, end, count)
	err = stream.Flush(ctx)
	if err != nil {
		t.Fatalf("Error from Flush %v", err)
	}
	if len(unflushed) == 0 {
		t.Fatal("Unflushed query was empty")
	}
	flushed, _, _ := query(t, ctx, stream, start, end, count)
	if len(flushed) == 0 {
		t.Fatal("Flushed query was empty")
	}
	calculated, err := helperMakeStatPoints(data, start, width)
	if err != nil {
		t.Fatalf("Error calculating expected query results: %v\n", err)
	}
	//This is here to help debug the test, at the moment it is not correct
	//as it does not calculate empty windows
	fmt.Printf("calculated:\n")
	spew.Dump(calculated)
	fmt.Printf("unflushed:\n")
	spew.Dump(unflushed)
	fmt.Printf("flushed:\n")
	spew.Dump(flushed)
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
	width := int64(end-start) / 1000
	result, version := helperWindowQuery(t, ctx, s, start, end+width, uint64(width), 0, 0)
	return result, version, width
}

func doAlignedWindowsQuery(t *testing.T, ctx context.Context, s *btrdb.Stream, start int64, end int64, count int64) ([]btrdb.StatPoint, uint64, int64) {
	pwe := uint8(35)
	width := int64(1) << pwe
	result, version := helperStatisticalQuery(t, ctx, s, start, end+width, pwe, 0)
	return result, version, width
}

func TestWindowsQueryWithHole(t *testing.T) {
	RunTestQueryWithHoles(t, doWindowsQuery, 100000)
}

func TestdWindowsAligneQueryWithHole(t *testing.T) {
	RunTestQueryWithHoles(t, doAlignedWindowsQuery, 100000)
}

func TestWindowsQueryFlushing(t *testing.T) {
	RunTestQueryFlushing(t, doWindowsQuery, 100000)
}

func TestWindowsAlignedQueryFlushing(t *testing.T) {
	RunTestQueryFlushing(t, doAlignedWindowsQuery, 100000)
}

//The database would actually flush an insert of 100k points. To properly test
//unflushed inserts we need to use less points:

func TestWindowsQueryNoInitFlushing(t *testing.T) {
	RunTestQueryFlushing(t, doWindowsQuery, 500)
}

func TestWindowsAlignedQueryNoInitFlushing(t *testing.T) {
	RunTestQueryFlushing(t, doAlignedWindowsQuery, 500)
}
