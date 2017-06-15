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

/*
// Use this version when using rocksdb as an embedded library (c-rocksdb)
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
*/

// Use this version when using rocksdb as a shared library
// Create builds an openable snapshot of RocksDB on the same disk.
// The directory should not already exist and should be an absolute path.
// log_size_for_flush: if the total log file size is equal or larger than
// this value, then a flush is triggered for all the column families. The
// default value is 0, which means flush is always triggered. If you move
// away from the default, the checkpoint may not contain up-to-date data
// if WAL writing is not always enabled.
func (c *Checkpoint) Create(checkpointDir string, logSizeForFlush uint64) error {
	var (
		cErr *C.char
		cDir = C.CString(checkpointDir)
	)
	defer C.free(unsafe.Pointer(cDir))
	C.rocksdb_checkpoint_create(c.c, cDir, C.uint64_t(logSizeForFlush), &cErr)
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
