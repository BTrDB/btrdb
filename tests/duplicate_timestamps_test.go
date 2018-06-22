package tests

import (
	"context"
	"gopkg.in/BTrDB/btrdb.v4"
	"testing"
	"time"
)

func TestInsertDuplciateTimestampsErrors(t *testing.T) {
	ctx := context.Background()
	db := helperConnect(t, ctx)

	firstStream := helperCreateDefaultStream(t, ctx, db, nil, nil)
	secondStream := helperCreateDefaultStream(t, ctx, db, nil, nil)
	start := int64(1524520800000)
	end := start + 100
	normalData := helperRandomDataCount(start, end, 100)

	err := firstStream.Insert(ctx, normalData)
	if err != nil {
		t.Fatalf("Error inserting normal data: %v", err)
	}

	duplicateData := make([]btrdb.RawPoint, 1000)
	for i, _ := range duplicateData {
		point := btrdb.RawPoint{start, 555}
		duplicateData[i] = point
	}
	timedCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
	err = secondStream.Insert(timedCtx, duplicateData)
	cancel()
	if err != nil {
		t.Fatalf("Error inserting duplicate timestamp data: %v", err)
	}

	pc, _, errc := firstStream.RawValues(ctx, start, end, 0)
	err = <-errc
	if err != nil {
		t.Fatalf("Failed querying first stream after duplicate insert: %v", err)
	}

	rv := make([]btrdb.RawPoint, 0)
	for p := range pc {
		rv = append(rv, p)
	}
	if len(rv) != len(normalData) {
		t.Fatal("Queried data len was different from original data len")
	}
}
