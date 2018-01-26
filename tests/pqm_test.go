package tests

import (
	"context"
	"fmt"
	"testing"

	"github.com/pborman/uuid"
	btrdb "gopkg.in/btrdb.v4"
)

func TestNoFlushInsert(t *testing.T) {
	db, err := btrdb.Connect(context.TODO(), btrdb.EndpointsFromEnv()...)
	if err != nil {
		t.Fatalf("Unexpected connection error: %v", err)
	}

	uu := uuid.NewRandom()
	stream, err := db.Create(context.Background(), uu, fmt.Sprintf("test.%x", uu[:]), nil, nil)
	if err != nil {
		t.Fatalf("create error %v", err)
	}
	vals := []btrdb.RawPoint{}
	for i := 0; i < 100; i++ {
		vals = append(vals, btrdb.RawPoint{Time: int64(i), Value: float64(i)})
	}
	err = stream.Insert(context.Background(), vals)
	if err != nil {
		t.Fatalf("unexpected error %v\n", err)
	}
	rvals, _, cerr := stream.RawValues(context.Background(), 0, 100, btrdb.LatestVersion)
	rvall := []btrdb.RawPoint{}
	for v := range rvals {
		rvall = append(rvall, v)
	}
	if e := <-cerr; e != nil {
		t.Fatalf("unexpected error %v\n", err)
	}
	if len(rvall) != 100 {
		t.Fatalf("only got %d points, wanted 100", len(rvall))
	}
}

func TestNoFlushInsertBiggerThanBuffer(t *testing.T) {
	db, err := btrdb.Connect(context.TODO(), btrdb.EndpointsFromEnv()...)
	if err != nil {
		t.Fatalf("Unexpected connection error: %v", err)
	}

	uu := uuid.NewRandom()
	stream, err := db.Create(context.Background(), uu, fmt.Sprintf("test.%x", uu[:]), nil, nil)
	if err != nil {
		t.Fatalf("create error %v", err)
	}
	vals := []btrdb.RawPoint{}
	for i := 0; i < 20000; i++ {
		vals = append(vals, btrdb.RawPoint{Time: int64(i), Value: float64(i)})
	}
	err = stream.Insert(context.Background(), vals)
	if err != nil {
		t.Fatalf("unexpected error %v\n", err)
	}
	rvals, _, cerr := stream.RawValues(context.Background(), 0, 20000, btrdb.LatestVersion)
	rvall := []btrdb.RawPoint{}
	for v := range rvals {
		rvall = append(rvall, v)
	}
	if e := <-cerr; e != nil {
		t.Fatalf("unexpected error %v\n", err)
	}
	if len(rvall) != 20000 {
		t.Fatalf("only got %d points, wanted 20000", len(rvall))
	}
}
