package tests

import (
	"context"
	"fmt"
	"testing"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"

	"gopkg.in/BTrDB/btrdb.v4"
)

func TestUsage(t *testing.T) {
	db, err := btrdb.Connect(context.TODO(), btrdb.EndpointsFromEnv()...)
	if err != nil {
		t.Fatalf("Unexpected connection error: %v", err)
	}
	anns := make(map[string]int)
	tags := make(map[string]int)
	for i := 0; i < 300; i++ {
		k := fmt.Sprintf("t%d", i)
		tags[k] = 1
		k = fmt.Sprintf("a%d", i)
		anns[k] = 1
	}
	for k, _ := range anns {
		uu := uuid.NewRandom()
		stream, err := db.Create(context.Background(), uu, fmt.Sprintf("usagetest/%x", uu[:]), nil, btrdb.M{k: "tst"})
		if err != nil {
			t.Fatalf("create error %v", err)
		}
		_ = stream
	}
	for k, _ := range tags {
		uu := uuid.NewRandom()
		stream, err := db.Create(context.Background(), uu, fmt.Sprintf("usagetest/%x", uu[:]), btrdb.M{k: "tst"}, nil)
		if err != nil {
			t.Fatalf("create error %v", err)
		}
		_ = stream
	}
	rvtags, rvanns, err := db.GetMetadataUsage(context.Background(), "usagetest/")
	require.NoError(t, err)
	require.EqualValues(t, 300, len(rvtags))
	require.EqualValues(t, 300, len(rvanns))
	rvtags, rvanns, err = db.GetMetadataUsage(context.Background(), "usagetest2/")
	require.NoError(t, err)
	require.EqualValues(t, 0, len(rvtags))
	require.EqualValues(t, 0, len(rvanns))
}
