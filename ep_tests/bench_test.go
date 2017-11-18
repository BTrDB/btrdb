package tests

import (
	"context"
	"fmt"
	"testing"

	btrdb "gopkg.in/BTrDB/btrdb.v4"

	"github.com/pborman/uuid"
)

func BenchmarkBigList(b *testing.B) {
	b.SkipNow()
	db := _connect(nil)
	colseed := uuid.NewRandom()
	b.ResetTimer()
	for col := 0; col < 10000; col++ {
		for s := 0; s < 50; s++ {
			uu := uuid.NewRandom()
			name := fmt.Sprintf("test.big.%x.%d", colseed[:], col)
			err := db.Create(context.Background(), uu, name, btrdb.M{"streamindex": fmt.Sprintf("%d", s)}, []byte("an annotation"))
			fmt.Printf("made col=%d s=%d err=%v\n", col, s, err)
			if err != nil {
				b.Fatalf("Creation error: %v", err)
			}
		}
	}

	for i := 0; i < b.N; i++ {
		rv, err := db.ListAllCollections(context.Background())
		if err != nil {
			b.Fatalf("List error: %v", err)
		}
		_ = rv
	}
}
func BenchmarkCreate(b *testing.B) {
	db := _connect(nil)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		uu := uuid.NewRandom()

		name := fmt.Sprintf("test.%x", uu[:])
		err := db.Create(context.Background(), uu, name, nil, nil)
		if err != nil {
			b.Fatalf("Creation error: %v", err)
		}
	}
}
func BenchmarkList(b *testing.B) {
	db := _connect(nil)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rv, err := db.ListAllCollections(context.Background())
		if err != nil {
			b.Fatalf("List error: %v", err)
		}
		_ = rv
	}
}
func BenchmarkListStreams(b *testing.B) {
	db := _connect(nil)
	rv, err := db.ListCollections(context.Background(), "test.big", "", 100)
	if err != nil || len(rv) != 100 {
		b.Fatalf("Got err %v or bad len %d", err, len(rv))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		strm, err := db.ListAllStreams(context.Background(), rv[i%100])
		if err != nil {
			b.Fatalf("got error listing streams %v", err)
		}
		_ = strm
	}
}
func BenchmarkDeepList(b *testing.B) {
	b.Skip()
	db := _connect(nil)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rv, err := db.ListAllCollections(context.Background())
		if err != nil {
			b.Fatalf("List error: %v", err)
		}
		total := 0
		for _, st := range rv {
			strm, err := db.ListAllStreams(context.Background(), st)
			if err != nil {
				b.Fatalf("Enumerate error %v", err)
			}
			total += len(strm)
		}
		fmt.Printf("There are %d collections and %d streams\n", len(rv), total)
	}
}
