package gorocksdb

// #include <stdlib.h>
// #include "rocksdb/c.h"
import "C"
import (
        "errors"
        "unsafe"
)

//Checkpoint is an openable snapshot of a database at a point in time.
type Checkpoint struct {
	c *C.rocksdb_checkpoint_t
}

// Create builds an openable snapshot of RocksDB on the same disk.
// The directory should not already exist and should be an absolute path.
func (c *Checkpoint) Create(checkpointDir string) error {
	var (
		cErr *C.char
		cDir = C.CString(checkpointDir)
	)
	defer C.free(unsafe.Pointer(cDir))
	C.rocksdb_checkpoint_create(c.c, cDir, &cErr)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		return errors.New(C.GoString(cErr))
	}
	return nil
}

// Destroy deallocates the checkpoint object.
func (c *Checkpoint) Destroy() {
	C.rocksdb_checkpoint_object_destroy(c.c)
	c.c = nil
}
