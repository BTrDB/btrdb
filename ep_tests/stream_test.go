package tests

import (
	"context"
	"fmt"
	"testing"

	"github.com/pborman/uuid"
	btrdb "gopkg.in/BTrDB/btrdb.v4"
)

func TestDoubleCreateExact(t *testing.T) {
	db := _connect(t)
	uu := uuid.NewRandom()
	name := fmt.Sprintf("test.%x", uu[:])
	err := db.Create(context.Background(), uu, name, nil, nil)
	if err != nil {
		t.Fatalf("Creation error: %v", err)
	}
	err = db.Create(context.Background(), uu, name, nil, nil)
	if err == nil {
		t.Fatalf("We were expecting an error, but didn't get one")
	}
}
func TestDoubleCreateDupUuid(t *testing.T) {
	db := _connect(t)
	uu := uuid.NewRandom()
	name := fmt.Sprintf("test.%x", uu[:])
	err := db.Create(context.Background(), uu, name, nil, nil)
	if err != nil {
		t.Fatalf("Creation error: %v", err)
	}
	name = fmt.Sprintf("test.alt.%x", uu[:])
	err = db.Create(context.Background(), uu, name, nil, nil)
	if err == nil {
		t.Fatalf("We were expecting an error, but didn't get one")
	}
}
func TestDoubleCreateDiffTags(t *testing.T) {
	db := _connect(t)
	uu := uuid.NewRandom()
	name := fmt.Sprintf("test.%x", uu[:])
	err := db.Create(context.Background(), uu, name, btrdb.M{"foo": "bar"}, nil)
	if err != nil {
		t.Fatalf("Creation error: %v", err)
	}
	uu = uuid.NewRandom()
	err = db.Create(context.Background(), uu, name, btrdb.M{"foo": "baz"}, nil)
	if err != nil {
		t.Fatalf("Creation error: %v", err)
	}
}

func TestDoubleCreateDiffTagsSameUU(t *testing.T) {
	db := _connect(t)
	uu := uuid.NewRandom()
	name := fmt.Sprintf("test.%x", uu[:])
	err := db.Create(context.Background(), uu, name, btrdb.M{"foo": "bar"}, nil)
	if err != nil {
		t.Fatalf("Creation error: %v", err)
	}
	err = db.Create(context.Background(), uu, name, btrdb.M{"foo": "baz"}, nil)
	if err == nil {
		t.Fatalf("Expected error")
	}
}

//BUG(mpa) this test fails, we clearly messed up the logic
func TestDoubleCreateSameTagsDiffUU(t *testing.T) {
	db := _connect(t)
	uu := uuid.NewRandom()
	name := fmt.Sprintf("test.%x", uu[:])
	err := db.Create(context.Background(), uu, name, btrdb.M{"foo": "bar"}, nil)
	if err != nil {
		t.Fatalf("Creation error: %v", err)
	}
	uu = uuid.NewRandom()
	err = db.Create(context.Background(), uu, name, btrdb.M{"foo": "bar"}, nil)
	if err == nil {
		t.Fatalf("Expected error")
	}
}
