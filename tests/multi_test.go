package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	btrdb "gopkg.in/btrdb.v4"

	"github.com/pborman/uuid"
	pb "gopkg.in/btrdb.v4/grpcinterface"
)

func TestCDC(t *testing.T) {
	db := _connect(t)
	inf, err := db.Info(context.Background())
	if err != nil {
		t.Fatalf("Problems abound %v", err)
	}
	total := 0
	for i := 0; i < 1000; i++ {
		for _, mbr := range inf.Members {
			if mbr.Up && mbr.In {
				mdb := _connectMbr(t, mbr)
				ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
				mi, err := mdb.Info(ctx)
				if err != nil {
					t.Fatalf("problem getting info")
				} else {
					total++
				}
				_ = mi
				cancel()
				mdb.Disconnect()
			}
		}
	}
	if total < 1000 {
		t.Fatalf("Inconsistent connect disconnect result: %v", total)
	}
}
func TestWrongEndpointCreate(t *testing.T) {
	db := _connect(t)
	inf, err := db.Info(context.Background())
	if err != nil {
		t.Fatalf("Info error %v", err)
	}
	upAndIn := 0
	for _, mbr := range inf.Members {
		if mbr.Up && mbr.In {
			upAndIn++
		}
	}
	if upAndIn < 2 {
		t.Fatalf("cannot do wrong endpoint test: <2 up+in members")
	}
	uuz := []uuid.UUID{}
	for i := 0; i < 100; i++ {
		uuz = append(uuz, uuid.NewRandom())
	}
	testran := false
	for _, uu := range uuz {
		//find a member that does not match
		for _, mbr := range inf.Members {
			if !mbr.In || !mbr.Up {
				//Skip, this member is out
				continue
			}
			if mbr != inf.EndpointFor(uu) {
				//This is the WRONG endpoint
				mdb := _connectMbr(t, mbr)
				//fmt.Printf("Connected member: %s", mbr.GetGrpcEndpoints())
				err := mdb.Create(context.Background(), uu, fmt.Sprintf("t%x", uu[:]), nil)
				if err == nil || btrdb.ToCodedError(err).Code != 405 {
					t.Fatalf("Expected wrong endpoint error, got %v", err)
				} else {
					testran = true
				}
				mdb.Disconnect()
			} else {
				//Skip, this is the right member
				continue
			}
		}
	}
	if !testran {
		t.Fatalf("Test did not run correctly")
	}
	//
	// 	values := []*pb.RawPoint{}
	// 	for i := 0; i < 100; i++ {
	// 		values = append(values, &pb.RawPoint{int64(i), float64(i) * 10.0})
	// 	}
	// 	err := db.Create(context.Background(), uuz[i], fmt.Sprintf("t%x", uuz[i][:]), nil)
	// 	if err != nil {
	// 		t.Fatalf("could not create ")
	// 	}
	// }
	// uu := uuid.NewRandom()
	// values := []*pb.RawPoint{}
	// for i := 0; i < 100; i++ {
	// 	values = append(values, &pb.RawPoint{int64(i), float64(i) * 10.0})
	// }
	// err := db.Insert(context.Background(), uu, values)
	// if err == nil {
	// 	t.Fatalf("Insert before create did not cause error: %s", err.Error())
	// }
}

func TestWrongEndpointInsert(t *testing.T) {
	db := _connect(t)
	inf, err := db.Info(context.Background())
	if err != nil {
		t.Fatalf("Info error %v", err)
	}
	upAndIn := 0
	for _, mbr := range inf.Members {
		if mbr.Up && mbr.In {
			upAndIn++
		}
	}
	if upAndIn < 2 {
		t.Fatalf("cannot do wrong endpoint test: <2 up+in members")
	}
	uuz := []uuid.UUID{}
	for i := 0; i < 100; i++ {
		uuz = append(uuz, uuid.NewRandom())
	}
	testran := false
	for _, uu := range uuz {
		//find a member that does not match
		for _, mbr := range inf.Members {
			if !mbr.In || !mbr.Up {
				//Skip, this member is out
				continue
			}
			if mbr == inf.EndpointFor(uu) {
				//This is the RIGHT endpoint
				mdb := _connectMbr(t, mbr)
				//fmt.Printf("Connected member: %s", mbr.GetGrpcEndpoints())
				err := mdb.Create(context.Background(), uu, fmt.Sprintf("t%x", uu[:]), nil)
				if err != nil {
					t.Fatalf("Unexpected error, got %v", err)
				}
				mdb.Disconnect()
			}
		}
	}

	for _, uu := range uuz {
		//find a member that does not match
		for _, mbr := range inf.Members {
			if !mbr.In || !mbr.Up {
				//Skip, this member is out
				continue
			}
			if mbr != inf.EndpointFor(uu) {
				//This is the WRONG endpoint
				mdb := _connectMbr(t, mbr)
				values := []*pb.RawPoint{}
				for i := 0; i < 100; i++ {
					values = append(values, &pb.RawPoint{int64(i), float64(i) * 10.0})
				}
				err := mdb.Insert(context.Background(), uu, values)
				if err == nil || btrdb.ToCodedError(err).Code != 405 {
					t.Fatalf("Expected wrong endpoint error, got %v", err)
				} else {
					testran = true
				}
				mdb.Disconnect()
			}
		}
	}

	if !testran {
		t.Fatalf("Test did not run correctly")
	}
	//
	// 	values := []*pb.RawPoint{}
	// 	for i := 0; i < 100; i++ {
	// 		values = append(values, &pb.RawPoint{int64(i), float64(i) * 10.0})
	// 	}
	// 	err := db.Create(context.Background(), uuz[i], fmt.Sprintf("t%x", uuz[i][:]), nil)
	// 	if err != nil {
	// 		t.Fatalf("could not create ")
	// 	}
	// }
	// uu := uuid.NewRandom()
	// values := []*pb.RawPoint{}
	// for i := 0; i < 100; i++ {
	// 	values = append(values, &pb.RawPoint{int64(i), float64(i) * 10.0})
	// }
	// err := db.Insert(context.Background(), uu, values)
	// if err == nil {
	// 	t.Fatalf("Insert before create did not cause error: %s", err.Error())
	// }
}
