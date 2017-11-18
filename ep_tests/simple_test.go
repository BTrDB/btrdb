package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/pborman/uuid"
	btrdb "gopkg.in/BTrDB/btrdb.v4"
	pb "gopkg.in/BTrDB/btrdb.v4/grpcinterface"
)

func TestEndpointsFromEnv(t *testing.T) {
	endpoints := btrdb.EndpointsFromEnv()
	if len(endpoints) == 0 {
		t.Fatal("BTRDB_ENDPOINTS not set")
	}
}
func TestConnect(t *testing.T) {
	db, err := btrdb.Connect(context.Background(), btrdb.EndpointsFromEnv()...)
	if err != nil {
		t.Fatalf("Could not connect: %v", err)
	}
	if db == nil {
		t.Fatalf("Database handle is nil")
	}
}
func _connect(t *testing.T) *btrdb.Endpoint {
	db, err := btrdb.ConnectEndpoint(context.Background(), btrdb.EndpointsFromEnv()...)
	if err != nil {
		if t == nil {
			panic(err)
		} else {
			t.Fatalf("Could not connect: %v", err)
		}
	}
	return db
}
func _connectMbr(t *testing.T, m *pb.Member) *btrdb.Endpoint {
	if m.GetGrpcEndpoints() == "" {
		t.Fatalf("Member GRPC endpoints unpopulated")
	}
	db, err := btrdb.ConnectEndpoint(context.Background(), m.GetGrpcEndpoints())
	if err != nil {
		if t == nil {
			panic(err)
		} else {
			t.Fatalf("Could not connect: %v", err)
		}
	}
	return db
}
func TestInsertBeforeCreate(t *testing.T) {
	db := _connect(t)
	uu := uuid.NewRandom()
	values := []*pb.RawPoint{}
	for i := 0; i < 100; i++ {
		values = append(values, &pb.RawPoint{Time: int64(i), Value: float64(i) * 10.0})
	}
	err := db.Insert(context.Background(), uu, values)
	if err == nil {
		t.Fatalf("Insert before create did not cause error: %s", err.Error())
	}
}
func TestInsertQueryRaw(t *testing.T) {
	db := _connect(t)
	uu := uuid.NewRandom()

	name := fmt.Sprintf("test.%x", uu[:])
	err := db.Create(context.Background(), uu, name, nil, nil)
	if err != nil {
		t.Fatalf("Creation error: %v", err)
	}
	values := []*pb.RawPoint{}
	for i := 0; i < 100; i++ {
		values = append(values, &pb.RawPoint{Time: int64(i), Value: float64(i) * 10.0})
	}
	err = db.Insert(context.Background(), uu, values)
	if err != nil {
		t.Fatalf("Insert error: %s", err.Error())
	}
	time.Sleep(6 * time.Second)
	vals, _, errs := db.RawValues(context.Background(), uu, 0, 88, btrdb.LatestVersion)
	count := 0
	var lastv btrdb.RawPoint
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
	name := fmt.Sprintf("test.%x", uu[:])
	err := db.Create(context.Background(), uu, name, nil, nil)
	if err != nil {
		t.Fatalf("Creation error: %v", err)
	}
	values := []*pb.RawPoint{}
	for i := 0; i < 100; i++ {
		values = append(values, &pb.RawPoint{Time: int64(i), Value: float64(i) * 10.0})
	}
	err = db.Insert(context.Background(), uu, values)
	if err != nil {
		t.Fatalf("Insert error: %s", err.Error())
	}
	time.Sleep(6 * time.Second)
	svals, _, errs := db.Windows(context.Background(), uu, 0, 90, 10, 0, btrdb.LatestVersion)
	for v := range svals {
		if v.Count != 10 {
			t.Fatal("Odd window length")
		}
	}
	e := <-errs
	if e != nil {
		t.Fatalf("Read error: %v", e.Error())
	}
	svals, _, errs = db.AlignedWindows(context.Background(), uu, 0, 16, 3, btrdb.LatestVersion)
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
