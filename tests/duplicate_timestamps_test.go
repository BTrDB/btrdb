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
	normalData := helperRandomDataCount(start, start+100, 100)

	err := firstStream.Insert(ctx, normalData)
	if err != nil {
		t.Fatalf("Error inserting normal data: %v", err)
	}

	duplicateData := make([]btrdb.RawPoint, 50000)
	for i, _ := range duplicateData {
		point := btrdb.RawPoint{start, 555}
		duplicateData[i] = point
	}
	timedCtx, _ := context.WithTimeout(ctx, 1*time.Second)
	err = secondStream.Insert(timedCtx, duplicateData)
	if err != nil {
		t.Fatalf("Error inserting normal data: %v", err)
	}
}
