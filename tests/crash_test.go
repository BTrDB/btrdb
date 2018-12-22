//+build ignore

package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/BTrDB/btrdb"
	"github.com/pborman/uuid"
)

func TestCrashError2(t *testing.T) {
	t.Skip()
	db, err := btrdb.Connect(context.TODO(), btrdb.EndpointsFromEnv()...)
	if err != nil {
		t.Fatalf("Unexpected connection error: %v", err)
	}
	uu := uuid.NewRandom()
	ep, err := db.EndpointFor(context.Background(), uu)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	rv, err := ep.FaultInject(context.Background(), 1, nil)
	fmt.Printf("got rv=%#v err=%#v\n", rv, err)
	if err == nil {
		t.Fatalf("expected an error, got %v", err)
	}
}
func TestCrashError(t *testing.T) {
	t.Skip()
	//First part, insert data
	db, err := btrdb.Connect(context.TODO(), btrdb.EndpointsFromEnv()...)
	if err != nil {
		t.Fatalf("Unexpected connection error: %v", err)
	}

	uu := uuid.NewRandom()
	stream, err := db.Create(context.Background(), uu, fmt.Sprintf("test.%x", uu[:]), btrdb.OptKV("name", "n"), nil)
	if err != nil {
		t.Fatalf("create error %v", err)
	}
	vals := []btrdb.RawPoint{}
	for i := 0; i < 10000; i++ {
		vals = append(vals, btrdb.RawPoint{Time: int64(i), Value: float64(i)})
	}
	err = stream.Insert(context.Background(), vals)
	if err != nil {
		t.Fatalf("unexpected error %v\n", err)
	}
	time.Sleep(6 * time.Second)
	//Next part, trigger a delayed panic
	ep, err := db.EndpointFor(context.Background(), uu)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_, err = ep.FaultInject(context.Background(), 2, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	then := time.Now()
	for time.Now().Sub(then) < 4*time.Second {
		sp, _, cerr := stream.Windows(context.Background(), -1000, 50000, 10000, 0, btrdb.LatestVersion)
		count := 0
		for _ = range sp {
			count++
		}
		fmt.Printf("count: %d\n", count)
		err = <-cerr
		if count == 0 && err == nil {
			t.Fatalf("Expected an error, but got nothing")
		}
		if err != nil {
			fmt.Printf("got error %v", err)
		}
	}
}
