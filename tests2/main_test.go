package tests2

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/pborman/uuid"

	btrdb "gopkg.in/btrdb.v4"
)

//This will fail if ANY of the env enpoints are down
func TestConnectFast(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	db, err := btrdb.Connect(ctx, btrdb.EndpointsFromEnv()...)
	if err != nil {
		t.Fatalf("got connection error %v", err)
	}
	_ = db
}

//This should work if some endpoints are down
func TestConnectLong(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	db, err := btrdb.Connect(ctx, btrdb.EndpointsFromEnv()...)
	if err != nil {
		t.Fatalf("got connection error %v", err)
	}
	_ = db
}

func TestChangedRangeSameVer(t *testing.T) {
	db, err := btrdb.Connect(context.TODO(), btrdb.EndpointsFromEnv()...)
	if err != nil {
		t.Fatalf("Unexpected connection error: %v", err)
	}

	uu := uuid.NewRandom()
	stream, err := db.Create(context.Background(), uu, fmt.Sprintf("test.%x", uu[:]), nil, nil)
	if err != nil {
		t.Fatalf("create error %v", err)
	}
	iver, err := stream.Version(context.Background())
	if err != nil {
		t.Fatalf("got iver error: %v", err)
	}
	vals := make([]btrdb.RawPoint, 100)
	for i := 0; i < 100; i++ {
		vals[i].Time = int64(i)
		vals[i].Value = float64(i)
	}
	err = stream.Insert(context.Background(), vals)
	if err != nil {
		t.Fatalf("got insert error %v", err)
	}
	//Let it flush
	time.Sleep(6 * time.Second)
	vals = make([]btrdb.RawPoint, 100)
	for i := 300; i < 400; i++ {
		vals[i-300].Time = int64(i)
		vals[i-300].Value = float64(i)
	}
	err = stream.Insert(context.Background(), vals)
	if err != nil {
		t.Fatalf("got insert2 error %v", err)
	}
	time.Sleep(6 * time.Second)
	ver, err := stream.Version(context.Background())
	if err != nil {
		t.Fatalf("got ver error %v", err)
	}
	if ver != 12 {
		t.Fatalf("expected two version to have happened iver=%d, ver=%d", iver, ver)
	}
	count := 0
	cr, _, cerr := stream.Changes(context.Background(), ver, ver, 0)

	for r := range cr {
		count++
		fmt.Printf("Got CR: %v", r)
	}
	if err := <-cerr; err != nil {
		t.Fatalf("got changed range error: %v", err)
	}
	if count != 0 {
		t.Fatalf("Did not get empty set for changed range on same version")
	}
}
func TestChangedRangeDiffVer(t *testing.T) {
	db, err := btrdb.Connect(context.TODO(), btrdb.EndpointsFromEnv()...)
	if err != nil {
		t.Fatalf("Unexpected connection error: %v", err)
	}

	uu := uuid.NewRandom()
	stream, err := db.Create(context.Background(), uu, fmt.Sprintf("test.%x", uu[:]), nil, nil)
	if err != nil {
		t.Fatalf("create error %v", err)
	}
	iver, err := stream.Version(context.Background())
	if err != nil {
		t.Fatalf("got iver error: %v", err)
	}
	vals := make([]btrdb.RawPoint, 100)
	for i := 0; i < 100; i++ {
		vals[i].Time = int64(i)
		vals[i].Value = float64(i)
	}
	err = stream.Insert(context.Background(), vals)
	if err != nil {
		t.Fatalf("got insert error %v", err)
	}
	//Let it flush
	time.Sleep(6 * time.Second)
	vals = make([]btrdb.RawPoint, 100)
	for i := 300; i < 400; i++ {
		vals[i-300].Time = int64(i)
		vals[i-300].Value = float64(i)
	}
	err = stream.Insert(context.Background(), vals)
	if err != nil {
		t.Fatalf("got insert2 error %v", err)
	}
	time.Sleep(6 * time.Second)
	ver, err := stream.Version(context.Background())
	if err != nil {
		t.Fatalf("got ver error %v", err)
	}
	if ver != 12 {
		t.Fatalf("expected two version to have happened iver=%d, ver=%d", iver, ver)
	}
	count := 0
	cr, _, cerr := stream.Changes(context.Background(), ver-1, ver, 0)

	for r := range cr {
		count++
		fmt.Printf("Got CR: %v", r)
	}
	if err := <-cerr; err != nil {
		t.Fatalf("got changed range error: %v", err)
	}
	if count == 0 {
		t.Fatalf("Got empty for different version")
	}
}
func TestListCollections(t *testing.T) {
	db, err := btrdb.Connect(context.TODO(), btrdb.EndpointsFromEnv()...)
	if err != nil {
		t.Fatalf("Unexpected connection error: %v", err)
	}
	cols, err := db.ListAllCollections(context.Background())
	if err != nil {
		t.Fatalf("Unexpected list error: %v", err)
	}
	for i, c := range cols {
		fmt.Printf("%d: %s\n", i, c)
	}
}
func TestConnectDudEndpoints(t *testing.T) {
	//Internally there is a 2 second timeout for a dud endpoint, don't exceed that
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	eps := []string{"192.168.123.123:4410"}
	eps = append(eps, btrdb.EndpointsFromEnv()...)
	db, err := btrdb.Connect(ctx, eps...)
	if err != nil {
		t.Fatalf("got connection error %v", err)
	}
	_ = db
}

func TestConnectDeadline(t *testing.T) {
	//Internally there is a 2 second timeout for a dud endpoint, don't exceed that
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	eps := []string{"192.168.123.123:4410", "192.168.123.124:4410"}
	eps = append(eps, btrdb.EndpointsFromEnv()...)
	db, err := btrdb.Connect(ctx, eps...)
	if err != context.DeadlineExceeded {
		t.Fatalf("got connection error %v, expected deadline exceeded", err)
	}
	_ = db
}

func TestInfo(t *testing.T) {
	db, err := btrdb.Connect(context.Background(), btrdb.EndpointsFromEnv()...)
	if err != nil {
		t.Fatalf("connection error %v", err)
	}
	info, err := db.Info(context.Background())
	if err != nil {
		t.Fatalf("unexpected eror %v", err)
	}
	if !info.Healthy {
		t.Fatalf("Cluster is not healthy %v", err)
	}
}

func TestNilRootAfterDeleteDelete(t *testing.T) {
	db, err := btrdb.Connect(context.TODO(), btrdb.EndpointsFromEnv()...)
	if err != nil {
		t.Fatalf("connection error %v", err)
	}
	uu := uuid.NewRandom()
	stream, err := db.Create(context.Background(), uu, fmt.Sprintf("test.%x", uu[:]), nil, nil)
	if err != nil {
		fmt.Sprintf("create error %v", err)
	}
	vals := make([]btrdb.RawPoint, 100)
	for i := 0; i < 100; i++ {
		vals[i].Time = int64(i)
		vals[i].Value = float64(i)
	}
	err = stream.Insert(context.Background(), vals)
	if err != nil {
		t.Fatalf("got insert error %v", err)
	}
	time.Sleep(6 * time.Second)
	valc, _, errc := stream.RawValues(context.Background(), 0, 98, btrdb.LatestVersion)
	count := 0
	for v := range valc {
		count++
		_ = v
	}
	err = <-errc
	if err != nil {
		t.Fatal("got insert error %v", err)
	}
	if count != 98 {
		t.Fatalf("Possible insert exclusion problem, got %v values expected %v", count, 98)
	}
	//Now delete it all
	ver, err := stream.DeleteRange(context.Background(), -100, 200)
	if err != nil {
		t.Fatal("delete error %v", err)
	}
	_ = ver
	//That should be synchronous
	//now try delete again
	ver, err = stream.DeleteRange(context.Background(), -100, 200)
	if err != nil {
		t.Fatal("delete error %v", err)
	}
	_ = ver
}

func TestNilRootAfterDeleteInsert(t *testing.T) {
	db, err := btrdb.Connect(context.TODO(), btrdb.EndpointsFromEnv()...)
	if err != nil {
		t.Fatalf("connection error %v", err)
	}
	uu := uuid.NewRandom()
	stream, err := db.Create(context.Background(), uu, fmt.Sprintf("test.%x", uu[:]), nil, nil)
	if err != nil {
		fmt.Sprintf("create error %v", err)
	}
	vals := make([]btrdb.RawPoint, 100)
	for i := 0; i < 100; i++ {
		vals[i].Time = int64(i)
		vals[i].Value = float64(i)
	}
	err = stream.Insert(context.Background(), vals)
	if err != nil {
		t.Fatalf("got insert error %v", err)
	}
	time.Sleep(6 * time.Second)
	valc, _, errc := stream.RawValues(context.Background(), 0, 98, btrdb.LatestVersion)
	count := 0
	for v := range valc {
		count++
		_ = v
	}
	err = <-errc
	if err != nil {
		t.Fatal("got insert error %v", err)
	}
	if count != 98 {
		t.Fatalf("Possible insert exclusion problem, got %v values expected %v", count, 98)
	}
	//Now delete it all
	ver, err := stream.DeleteRange(context.Background(), -100, 200)
	if err != nil {
		t.Fatal("delete error %v", err)
	}
	_ = ver
	//That should be synchronous
	//now try insert and query again
	err = stream.Insert(context.Background(), vals)
	if err != nil {
		t.Fatalf("got insert error %v", err)
	}
	time.Sleep(6 * time.Second)
	valc, _, errc = stream.RawValues(context.Background(), 0, 98, btrdb.LatestVersion)
	count = 0
	for v := range valc {
		count++
		_ = v
	}
	err = <-errc
	if err != nil {
		t.Fatal("got insert error %v", err)
	}
	if count != 98 {
		t.Fatalf("Possible insert exclusion problem, got %v values expected %v", count, 98)
	}
}

func TestNilRootAfterDeleteQueryRaw(t *testing.T) {
	db, err := btrdb.Connect(context.TODO(), btrdb.EndpointsFromEnv()...)
	if err != nil {
		t.Fatalf("connection error %v", err)
	}
	uu := uuid.NewRandom()
	stream, err := db.Create(context.Background(), uu, fmt.Sprintf("test.%x", uu[:]), nil, nil)
	if err != nil {
		fmt.Sprintf("create error %v", err)
	}
	vals := make([]btrdb.RawPoint, 100)
	for i := 0; i < 100; i++ {
		vals[i].Time = int64(i)
		vals[i].Value = float64(i)
	}
	err = stream.Insert(context.Background(), vals)
	if err != nil {
		t.Fatalf("got insert error %v", err)
	}
	time.Sleep(6 * time.Second)
	valc, _, errc := stream.RawValues(context.Background(), 0, 98, btrdb.LatestVersion)
	count := 0
	for v := range valc {
		count++
		_ = v
	}
	err = <-errc
	if err != nil {
		t.Fatal("got insert error %v", err)
	}
	if count != 98 {
		t.Fatalf("Possible insert exclusion problem, got %v values expected %v", count, 98)
	}
	//Now delete it all
	ver, err := stream.DeleteRange(context.Background(), -100, 200)
	if err != nil {
		t.Fatal("delete error %v", err)
	}
	_ = ver
	//That should be synchronous
	//now try query again
	valc, _, errc = stream.RawValues(context.Background(), 0, 99, btrdb.LatestVersion)
	count = 0
	for v := range valc {
		count++
		_ = v
	}
	err = <-errc
	if err != nil {
		t.Fatal("Got query error %v", err)
	}
	if count != 0 {
		t.Fatal("got unexpected count")
	}

}

func TestCreate(t *testing.T) {
	db, err := btrdb.Connect(context.TODO(), btrdb.EndpointsFromEnv()...)
	if err != nil {
		t.Fatalf("connection error %v", err)
	}
	//A couple to make sure we hit all endpoints
	for i := 0; i < 10; i++ {
		for s := 0; s < 10; s++ {
			uu := uuid.NewRandom()
			str, err := db.Create(context.Background(), uu, fmt.Sprintf("test.%x", uu[:]), btrdb.M{"s": fmt.Sprintf("%d", s)}, nil)
			if err != nil {
				t.Fatalf("got create error %v", err)
			}
			ver, err := str.Version(context.Background())
			if err != nil {
				t.Fatalf("got error querying version %v", err)
			}
			data, verc, errc := str.RawValues(context.Background(), 0, 100, btrdb.LatestVersion)
			count := 0
			for d := range data {
				count++
				_ = d
			}
			err = <-errc
			ver = <-verc
			if err != nil {
				t.Fatalf("got error querying raw values on created but empty stream %v", err)
			}
			if count != 0 {
				t.Fatalf("Got values from empty stream")
			}
			if ver != 10 {
				t.Fatalf("Expected version 10, got %v", ver)
			}
		}
	}
}
