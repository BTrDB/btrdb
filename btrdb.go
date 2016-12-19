package btrdb

//go:generate protoc pb/btrdb.proto --go_out=plugins=grpc:.
import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/pborman/uuid"
	"google.golang.org/grpc"
	pb "gopkg.in/btrdb.v4/grpcinterface"
)

type BTrDB struct {
	g pb.BTrDBClient
}

func NewBTrDBConnection(servers ...string) (*BTrDB, error) {
	conn, err := grpc.Dial(servers[0], grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	//	defer conn.Close()
	client := pb.NewBTrDBClient(conn)
	rv := &BTrDB{g: client}
	return rv, nil
}

func (b *BTrDB) GetGRPC() pb.BTrDBClient {
	return b.g
}

func (b *BTrDB) Insert(uu uuid.UUID, values []*pb.RawPoint) {
	rv, err := b.g.Insert(context.Background(), &pb.InsertParams{
		Uuid:   uu,
		Sync:   false,
		Values: values,
	})
	fmt.Println("got erri: ", err)
	fmt.Println("got stati: ", rv.GetStat())
	if err != nil {
		os.Exit(1)
	}
}

func (b *BTrDB) RawValues(uu uuid.UUID, start int64, end int64, version uint64) chan *pb.RawPoint {
	rv, err := b.g.RawValues(context.Background(), &pb.RawValuesParams{
		Uuid:         uu,
		Start:        start,
		End:          end,
		VersionMajor: version,
	})
	fmt.Println("got errv: ", err)
	if err != nil {
		panic(err)
	}
	rvc := make(chan *pb.RawPoint, 100)
	go func() {
		for {
			rawv, err := rv.Recv()
			if err == io.EOF {
				fmt.Println("donezies")
				close(rvc)
				return
			}
			fmt.Printf("stat was %#v\n", rawv.Stat)
			if err != nil {
				panic(err)
			}
			for _, x := range rawv.Values {
				fmt.Println("sending shits")
				rvc <- x
			}
		}
	}()
	return rvc
}
