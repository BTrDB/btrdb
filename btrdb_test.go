package btrdb

import (
	"testing"
	
	uuid "github.com/pborman/uuid"
)

var dbaddr string = "localhost:4410"

func TestBadStream(t *testing.T) {
	var uuid uuid.UUID = uuid.NewRandom()
	var err error
	var bc *BTrDBConnection
	var sv *StandardValue
	var svchan chan *StandardValue
	var version uint64
	var versionchan chan uint64
	
	bc, err = NewBTrDBConnection(dbaddr)
	if err != nil {
		t.Fatal(err)
	}
	svchan, versionchan, err = bc.QueryStandardValues(uuid, 10, 20, 0)
	if err != nil {
		t.Fatal(err)
	}
	
	version = <- versionchan
	sv = <- svchan
	
	if version != 0 || sv != nil {
		t.Fatal("Got something")
	}
}
