package gorocksdb

import (
        "testing"
	"os"
        "github.com/facebookgo/ensure"
)

func TestCheckpoint(t *testing.T) {
	db := newTestDB(t, "TestDBCheckpoint", nil)
	defer db.Close()

	var (
		givenKey = []byte("hello")
		givenVal = []byte("world")
		opts	 = NewDefaultOptions()
		wo	 = NewDefaultWriteOptions()
		ro	 = NewDefaultReadOptions()
	)

	ensure.Nil(t, db.Put(wo, givenKey, givenVal))

	// create a checkpoint object
	checkpoint, err := db.NewCheckpointObject()
	defer checkpoint.Destroy()
	ensure.Nil(t, err)

	// create an openable checkpoint of the database
	ensure.Nil(t, checkpoint.Create("gorocksdb-Checkpoint"))

	// open a new database from the checkpoint
	new_db, err := OpenDb(opts, "gorocksdb-Checkpoint")
	ensure.Nil(t,err)
	defer new_db.Close()

	// retrieve key
	v, err := new_db.Get(ro, givenKey)
	defer v.Free()
	ensure.Nil(t, err)
	ensure.DeepEqual(t, v.Data(), givenVal)
	os.RemoveAll("gorocksdb-Checkpoint")
}
