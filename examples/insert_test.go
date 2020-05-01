package examples

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/pborman/uuid"

	btrdb "github.com/BTrDB/btrdb/v5"
)

func TestInsertingProceduralData(t *testing.T) {
	//First connect to the cluster. In BTrDB v4 we are advocating that all
	//programs use environment variables to specify the endpoint rather
	//than assuming specific addresses:
	//Set $BTRDB_ENDPOINTS to
	//"server1:4410;server2:4410..."
	//Note that not all endpoints need be listed, but it will make this
	//program more resilient if you specify more or all of the endpoints
	db, err := btrdb.Connect(context.TODO(), btrdb.EndpointsFromEnv()...)
	if err != nil {
		t.Fatalf("Unexpected connection error: %v", err)
	}

	//Streams must be created before use
	uu := uuid.NewRandom()
	//A collection is a small group of streams (<100 is best) generally associated
	//with a single device or service. BTrDB is designed for lots of small collections
	//not small numbers of big collections
	collection := fmt.Sprintf("test/inserting_procedural_data.%d", time.Now().UnixNano())
	//Tags are used to identify streams within a collection
	val := "value"
	anotherVal := "anothervalue"
	tags := map[string]*string{"key": &val, "anotherkey": &anotherVal}
	//The annotation is used to store (mutable) extra data with the stream. It
	//is technically just a byte array, but we prefer people use msgpacked objects.
	//the tooling is not quite there to make this easy, so its ok to make this nil
	//for now
	var annotation map[string]*string = nil

	stream, err := db.Create(context.TODO(), uu, collection, tags, annotation)
	if err != nil {
		t.Fatalf("Unexpected creation error: %v", err)
	}

	//Now you manipulate the stream:
	err = stream.InsertTV(context.TODO(),
		[]int64{100e6, 200e6, 300e6, 400e6},
		[]float64{1.1, 2.2, 3.3, 4.4})
	if err != nil {
		t.Fatalf("Unexpected insert error: %v", err)
	}

	//Start = -1000ns, End = 1000ns, Width = 150ns, Depth = 2^0 (all the way), Version = latest
	rvchan, ver, errc := stream.Windows(context.TODO(), -1000e6, 1000e6, 150e6, 0, btrdb.LatestVersion)
	_ = ver //don't use this, that's ok
	for result := range rvchan {
		fmt.Printf("Window @%d min=%.2f mean=%.2f max=%.2f count=%d\n",
			result.Time, result.Min, result.Mean, result.Max, result.Count)
	}
	if e := <-errc; e != nil {
		t.Fatalf("Got an error: %v", e)
	}
}
