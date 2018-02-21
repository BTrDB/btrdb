package tests

import (
	"context"
	"testing"

	"gopkg.in/BTrDB/btrdb.v4"
)

type QueryFunc func(t *testing.T, ctx context.Context, s *btrdb.Stream, start int64, end int64, count int64) ([]btrdb.StatPoint, uint64, int64)

func RunTestQueryWithHoles(t *testing.T, query QueryFunc) {
	ctx := context.Background()
	db := helperConnect(t, ctx)
	stream := helperCreateDefaultStream(t, ctx, db, nil, nil)
	start := int64(1519088910) // Random unix datetime
	midEnd := start + 1000000
	midStart := midEnd + 100000
	finalEnd := midStart + 1000000
	count := int64(100000)
	firstData := helperRandomDataCount(start, midEnd, count)
	helperInsert(t, ctx, stream, firstData)
	secondData := helperRandomDataCount(midStart, finalEnd, count)
	helperInsert(t, ctx, stream, secondData)
	spts, _, width := query(t, ctx, stream, start, finalEnd, count*2)
	allData := make([]btrdb.RawPoint, 0)
	allData = append(allData, firstData...)
	allData = append(allData, secondData...)
	err := helperCheckStatisticalCorrect(allData, spts, int64(width))
	if err != nil {
		t.Fatalf("Queried data was invalid: %v", err)
	}
}

func RunTestQueryFlushing(t *testing.T, query QueryFunc) {
	ctx := context.Background()
	db := helperConnect(t, ctx)
	stream := helperCreateDefaultStream(t, ctx, db, nil, nil)
	start := int64(1519088910) // Random unix datetime
	end := start + 1000000
	count := int64(100000)
	data := helperRandomDataCount(start, end, count)
	err := stream.Insert(ctx, data)
	if err != nil {
		t.Fatalf("Error from insert %v", err)
	}
	unflushed, _, _ := query(t, ctx, stream, start, end, count)
	err = stream.Flush(ctx)
	if err != nil {
		t.Fatalf("Error from Flush %v", err)
	}
	flushed, _, _ := query(t, ctx, stream, start, end, count)
	err = helperCheckStatisticalEqual(unflushed, flushed)
	if err != nil {
		t.Fatal("Flushed and unflushed queries were not equal.")
	}
}

func doWindowsQuery(t *testing.T, ctx context.Context, s *btrdb.Stream, start int64, end int64, count int64) ([]btrdb.StatPoint, uint64, int64) {
	width := int64(end - start)
	result, version := helperWindowQuery(t, ctx, s, start, end+width, uint64(width), 0, 0)
	return result, version, width
}

func doAlignedWindowsQuery(t *testing.T, ctx context.Context, s *btrdb.Stream, start int64, end int64, count int64) ([]btrdb.StatPoint, uint64, int64) {
	pwe := uint8(48)
	width := int64(1) << pwe
	result, version := helperStatisticalQuery(t, ctx, s, start, end+width, pwe, 0)
	return result, version, width
}

func TestWindowsQueryWithHole(t *testing.T) {
	RunTestQueryWithHoles(t, doWindowsQuery)
}

func TestAlignedWindowsQueryWithHole(t *testing.T) {
	RunTestQueryWithHoles(t, doAlignedWindowsQuery)
}

func TestWindowsQueryFlushing(t *testing.T) {
	RunTestQueryFlushing(t, doWindowsQuery)
}

func TestAlignedWindowsQueryFlushing(t *testing.T) {
	RunTestQueryFlushing(t, doAlignedWindowsQuery)
}
