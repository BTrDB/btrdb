package tests

import (
	"context"
	"fmt"
	"testing"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"

	"github.com/BTrDB/btrdb"
)

func TestUsage(t *testing.T) {
	db, err := btrdb.Connect(context.TODO(), btrdb.EndpointsFromEnv()...)
	if err != nil {
		t.Fatalf("Unexpected connection error: %v", err)
	}
	anns := make(map[string]int)
	for i := 0; i < 300; i++ {
		k := fmt.Sprintf("a%d", i)
		anns[k] = 1
	}
	for k, _ := range anns {
		uu := uuid.NewRandom()
		stream, uerr := db.Create(context.Background(), uu, fmt.Sprintf("usagetest/%x", uu[:]), btrdb.OptKV("name", "n"), btrdb.OptKV(k, "tst"))
		if uerr != nil {
			t.Fatalf("create error %v", uerr)
		}
		_ = stream
	}
	rvtags, rvanns, err := db.GetMetadataUsage(context.Background(), "usagetest/")
	require.NoError(t, err)
	require.EqualValues(t, 3, len(rvtags))
	require.EqualValues(t, 300, len(rvanns))
	rvtags, rvanns, err = db.GetMetadataUsage(context.Background(), "usagetest2/")
	require.NoError(t, err)
	require.EqualValues(t, 3, len(rvtags))
	require.EqualValues(t, 0, len(rvanns))
}
