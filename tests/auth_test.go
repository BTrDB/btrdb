package tests

import (
	"context"
	"fmt"
	"testing"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"

	"github.com/BTrDB/btrdb"
)

func Test2Auth(t *testing.T) {
	t.Skip()
	ctx := context.Background()
	db, err := btrdb.Connect(ctx, btrdb.EndpointsFromEnv()...)
	if err != nil {
		t.Fatalf("Unexpected connection error: %v", err)
	}
	db.Create(context.Background(), uuid.NewRandom(), "test2/a/b", btrdb.M{"name": "n"}, nil)
	db.Create(context.Background(), uuid.NewRandom(), "test2/b/c", btrdb.M{"name": "n"}, nil)
}
func Test2AuthTry(t *testing.T) {
	ctx := context.Background()
	db, err := btrdb.ConnectAuth(ctx, "26A027F19AA246E196CF6CE0", btrdb.EndpointsFromEnv()...)
	require.NoError(t, err)
	cols, err := db.ListAllCollections(ctx)
	_ = cols
}
func testAuthAPICalls(ctx context.Context, t *testing.T, db *btrdb.BTrDB) {
	uu := uuid.NewRandom()
	stream, err := db.Create(ctx, uu, fmt.Sprintf("authtest/%x", uu[:]), btrdb.M{"name": "n"}, nil)
	if err != nil {
		t.Fatalf("create error %v", err)
	}
	if err != nil {
		fmt.Printf("CREATE\t\tfailed: %v\n", err)
		return
	}
	fmt.Printf("CREATE\t\tsucceeded\n")

	err = stream.Insert(ctx, []btrdb.RawPoint{btrdb.RawPoint{10, 10}})
	if err == nil {
		fmt.Printf("INSERT\t\tsucceeded\n")
	} else {
		fmt.Printf("INSERT\t\tfailed: %v\n", err)
		return
	}

	_, err = stream.Exists(ctx)
	if err == nil {
		fmt.Printf("EXISTS\t\tsucceeded\n")
	} else {
		fmt.Printf("EXISTS\t\tfailed: %v\n", err)
	}

	_, _, err = stream.Annotations(ctx)
	if err == nil {
		fmt.Printf("ANNOTATIONS\tsucceeded\n")
	} else {
		fmt.Printf("ANNOTATIONS\tfailed: %v\n", err)
	}
	err = stream.Flush(ctx)
	if err == nil {
		fmt.Printf("FLUSH\t\tsucceeded\n")
	} else {
		fmt.Printf("FLUSH\t\tfailed: %v\n", err)
	}

	sp, _, cerr := stream.AlignedWindows(ctx, 0, 100, 5, btrdb.LatestVersion)
	for _ = range sp {
		fmt.Printf("  Got alignedwindows Point\n")
	}
	err = <-cerr
	if err == nil {
		fmt.Printf("ALIGNEDWINDOWS\tsucceeded\n")
	} else {
		fmt.Printf("ALIGNEDWINDOWS\tfailed: %v\n", err)
	}

	sp, _, cerr = stream.Windows(ctx, 0, 100, 50, 0, btrdb.LatestVersion)
	for _ = range sp {
		fmt.Printf("  Got windows Point\n")
	}
	err = <-cerr
	if err == nil {
		fmt.Printf("WINDOWS\t\tsucceeded\n")
	} else {
		fmt.Printf("WINDOWS\t\tfailed: %v\n", err)
	}

	rp, _, cerr := stream.RawValues(ctx, 0, 100, btrdb.LatestVersion)
	for _ = range rp {
		fmt.Printf("  Got raw Point\n")
	}
	err = <-cerr
	if err == nil {
		fmt.Printf("RAW\t\tsucceeded\n")
	} else {
		fmt.Printf("RAW\t\tfailed: %v\n", err)
	}

	_, err = stream.DeleteRange(ctx, 0, 20)
	if err == nil {
		fmt.Printf("DELETE\t\tsucceeded\n")
	} else {
		fmt.Printf("DELETE\t\tfailed: %v\n", err)
	}

	err = stream.Obliterate(ctx)
	if err == nil {
		fmt.Printf("OBLITERATE\tsucceeded\n")
	} else {
		fmt.Printf("OBLITERATE\tfailed: %v\n", err)
	}

}
func TestAuthPublic(t *testing.T) {
	t.Skip()
	ctx := context.Background()
	db, err := btrdb.Connect(ctx, btrdb.EndpointsFromEnv()...)
	if err != nil {
		t.Fatalf("Unexpected connection error: %v", err)
	}
	testAuthAPICalls(ctx, t, db)
}

func TestAuthAPIK(t *testing.T) {
	t.Skip()
	//Fill in the username and API key to test
	const user = "tst"
	const apik = "255C59A06BB698681E3580D2"
	ctx := context.Background()
	db, err := btrdb.ConnectAuth(ctx, apik, btrdb.EndpointsFromEnv()...)
	if err != nil {
		t.Fatalf("Unexpected connection error: %v", err)
	}
	testAuthAPICalls(ctx, t, db)

}
