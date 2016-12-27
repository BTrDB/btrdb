package tests

import (
	"context"
	"fmt"
	"time"

	_ "github.com/ceph/go-ceph/rados"
	_ "github.com/coreos/etcd/clientv3"
	"github.com/pborman/uuid"
	btrdb "gopkg.in/btrdb.v4"
)

func main() {
	db, err := btrdb.Connect(btrdb.EndpointsFromEnv()...)
	if err != nil {
		panic(err)
	}
	for i := 0; i < 10; i++ {
		ii := i
		go func() {
			for k := 0; k < 100000; k++ {
				uu := uuid.NewRandom()
				col := fmt.Sprintf("huge.%x", uu[:])
				db.Create(context.Background(), uu, col, btrdb.M{"foo": "bar", "baz": "boop"})
				if k%100 == 0 {
					fmt.Printf("gr %d created %d\n", ii, k)
				}
			}
		}()
	}
	for {
		time.Sleep(10 * time.Second)
	}
}
