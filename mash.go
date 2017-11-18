package btrdb

import (
	"strings"

	"github.com/huichen/murmur"
	"github.com/pborman/uuid"
	pb "gopkg.in/BTrDB/btrdb.v4/grpcinterface"
)

//The MASH struct (Master Allocation by Stable Hashing) contains information
//about the cluster and which fraction of the uuid space is being served by
//which endpoints. Generally you will not need to use this, but it is handy
//for checking the cluster is healthy.
type MASH struct {
	*pb.Mash
	eps []mashEndpoint
}

type mashEndpoint struct {
	start int64
	end   int64
	hash  uint32
	grpc  []string
}

func (m *MASH) pbEndpointFor(uuid uuid.UUID) *pb.Member {
	hsh := int64(murmur.Murmur3(uuid[:]))
	for _, mbr := range m.GetMembers() {
		s := mbr.GetStart()
		e := mbr.GetEnd()
		if hsh >= s && hsh < e {
			return mbr
		}
	}
	return nil
}

//EndpointFor will take a uuid and return the connection details for the
//endpoint that can service a write to that uuid. This is a low level function.
func (m *MASH) EndpointFor(uuid uuid.UUID) (found bool, hash uint32, addrs []string) {
	hsh := int64(murmur.Murmur3(uuid[:]))
	for _, e := range m.eps {
		if e.start <= hsh && e.end > hsh {
			return true, e.hash, e.grpc
		}
	}
	return false, 0, nil
}

//Called after mash pb obj is populated. Can be used to fill in
//read optimized data structures in the MASH object
func (m *MASH) precalculate() {
	m.eps = []mashEndpoint{}
	for _, mbr := range m.Members {
		if mbr.In && mbr.Up && mbr.Start != mbr.End {
			seps := strings.Split(mbr.GrpcEndpoints, ";")
			m.eps = append(m.eps, mashEndpoint{
				start: mbr.Start,
				end:   mbr.End,
				hash:  mbr.Hash,
				grpc:  seps,
			})
		}
	}
}
