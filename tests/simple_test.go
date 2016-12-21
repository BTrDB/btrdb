package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/pborman/uuid"
	btrdb "gopkg.in/btrdb.v4"
	pb "gopkg.in/btrdb.v4/grpcinterface"
)

func TestEndpointsFromEnv(t *testing.T) {
	endpoints := btrdb.EndpointsFromEnv()
	if len(endpoints) == 0 {
		t.Fatal("BTRDB_ENDPOINTS not set")
	}
	fmt.Printf("%#v %d", endpoints, len(endpoints))
}
func TestConnect(t *testing.T) {
	db, err := btrdb.Connect(btrdb.EndpointsFromEnv()...)
	if err != nil {
		t.Fatalf("Could not connect: %v", err)
	}
	if db == nil {
		t.Fatalf("Database handle is nil")
	}
}
func _connect(t *testing.T) *btrdb.BTrDB {
	db, err := btrdb.Connect(btrdb.EndpointsFromEnv()...)
	if err != nil {
		t.Fatalf("Could not connect: %v", err)
	}
	return db
}
func TestInsertQueryRaw(t *testing.T) {
	db := _connect(t)
	uu := uuid.NewRandom()
	values := []*pb.RawPoint{}
	for i := 0; i < 100; i++ {
		values = append(values, &pb.RawPoint{int64(i), float64(i) * 10.0})
	}
	err := db.Insert(context.Background(), uu, values)
	if err != nil {
		t.Fatalf("Insert error: %s", err.Error())
	}
	time.Sleep(6 * time.Second)
	vals, errs := db.RawValues(context.Background(), uu, 0, 88, btrdb.LatestVersion)
	count := 0
	var lastv *pb.RawPoint
	for v := range vals {
		count++
		lastv = v
	}
	e := <-errs
	if e != nil {
		t.Fatalf("Read error: %v", e.Error())
	}
	if count != 88 {
		t.Fatalf("Read back wrong number of values %d expected %d (last %v/%v)", count, 88, lastv.Time, lastv.Value)
	}
}

func TestInsertQueryWindows(t *testing.T) {
	db := _connect(t)
	uu := uuid.NewRandom()
	values := []*pb.RawPoint{}
	for i := 0; i < 100; i++ {
		values = append(values, &pb.RawPoint{int64(i), float64(i) * 10.0})
	}
	err := db.Insert(context.Background(), uu, values)
	if err != nil {
		t.Fatalf("Insert error: %s", err.Error())
	}
	time.Sleep(6 * time.Second)
	svals, errs := db.Windows(context.Background(), uu, 0, 90, 10, 0, btrdb.LatestVersion)
	for v := range svals {
		if v.Count != 10 {
			t.Fatal("Odd window length")
		}
	}
	e := <-errs
	if e != nil {
		t.Fatalf("Read error: %v", e.Error())
	}
	svals, errs = db.AlignedWindows(context.Background(), uu, 0, 16, 3, btrdb.LatestVersion)
	count := 0
	for v := range svals {
		count++
		if v.Count != 8 {
			t.Fatalf("Odd aligned window length %d", v.Count)
		}
	}
	if count != 2 {
		t.Fatalf("Odd count, expected 2 got %d", count)
	}
}

//Test if pw 64 is okay
