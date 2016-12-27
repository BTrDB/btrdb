package tests

import (
	"context"
	"testing"
)

func TestGetInfo(t *testing.T) {
	db := _connect(t)
	inf, err := db.Info(context.Background())
	if err != nil {
		t.Fatalf("got error: %v\n", err)
	}
	_ = inf
	// for _, m := range inf.Members {
	// 	fmt.Printf(" member %#v\n", m)
	// }
}
