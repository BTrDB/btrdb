package btrdb

import (
	"strings"

	"github.com/huichen/murmur"
	"github.com/pborman/uuid"
	pb "gopkg.in/btrdb.v4/grpcinterface"
)

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

func (m *MASH) PbEndpointFor(uuid uuid.UUID) *pb.Member {
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

func (m *MASH) EndpointFor(uuid uuid.UUID) (bool, uint32, []string) {
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
