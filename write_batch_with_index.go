package gorocksdb

// #include <stdlib.h>
// #include "rocksdb/c.h"
import "C"
import (
	"errors"
	"unsafe"
)

// WriteBatchWithIndex is a batching of Puts, Merges and Deletes with a searchable
// index that supports read-your-own-writes
type WriteBatchWithIndex struct {
        c *C.rocksdb_writebatch_wi_t
}

// NewWriteBatchWithIndex creates a WriteBatchWithIndex object.
// reservedBytes : reserved bytes in underlying WriteBatch
// overwriteKey: if true, overwrite the key in the index when inserting the same 
// key as previously, so iterator will never show two entries with the same key.
func NewWriteBatchWithIndex(reservedBytes int, overwriteKey bool) *WriteBatchWithIndex {
	var (
		cReservedBytes	= C.size_t(reservedBytes)
		cOverwriteKey	= boolToChar(overwriteKey)
	)
        return NewNativeWriteBatchWithIndex(C.rocksdb_writebatch_wi_create(cReservedBytes, cOverwriteKey))
}

// NewNativeWriteBatchWithIndex creates a WriteBatchWithIndex object.
func NewNativeWriteBatchWithIndex(c *C.rocksdb_writebatch_wi_t) *WriteBatchWithIndex {
        return &WriteBatchWithIndex{c}
}

// Put queues a key-value pair.
func (wb *WriteBatchWithIndex) Put(key, value []byte) {
        cKey := byteToChar(key)
        cValue := byteToChar(value)
        C.rocksdb_writebatch_wi_put(wb.c, cKey, C.size_t(len(key)), cValue, C.size_t(len(value)))
}

// PutCF queues a key-value pair in a column family.
func (wb *WriteBatchWithIndex) PutCF(cf *ColumnFamilyHandle, key, value []byte) {
        cKey := byteToChar(key)
        cValue := byteToChar(value)
        C.rocksdb_writebatch_wi_put_cf(wb.c, cf.c, cKey, C.size_t(len(key)), cValue, C.size_t(len(value)))
}

// Merge queues a merge of "value" with the existing value of "key".
func (wb *WriteBatchWithIndex) Merge(key, value []byte) {
        cKey := byteToChar(key)
        cValue := byteToChar(value)
        C.rocksdb_writebatch_wi_merge(wb.c, cKey, C.size_t(len(key)), cValue, C.size_t(len(value)))
}

// MergeCF queues a merge of "value" with the existing value of "key" in a
// column family.
func (wb *WriteBatchWithIndex) MergeCF(cf *ColumnFamilyHandle, key, value []byte) {
        cKey := byteToChar(key)
        cValue := byteToChar(value)
        C.rocksdb_writebatch_wi_merge_cf(wb.c, cf.c, cKey, C.size_t(len(key)), cValue, C.size_t(len(value)))
}

// Delete queues a deletion of the data at key.
func (wb *WriteBatchWithIndex) Delete(key []byte) {
        cKey := byteToChar(key)
        C.rocksdb_writebatch_wi_delete(wb.c, cKey, C.size_t(len(key)))
}

// DeleteCF queues a deletion of the data at key in a column family.
func (wb *WriteBatchWithIndex) DeleteCF(cf *ColumnFamilyHandle, key []byte) {
        cKey := byteToChar(key)
        C.rocksdb_writebatch_wi_delete_cf(wb.c, cf.c, cKey, C.size_t(len(key)))
}

// Data returns the serialized version of this batch.
func (wb *WriteBatchWithIndex) Data() []byte {
        var cSize C.size_t
        cValue := C.rocksdb_writebatch_wi_data(wb.c, &cSize)
        return charToByte(cValue, cSize)
}

// Count returns the number of updates in the batch.
func (wb *WriteBatchWithIndex) Count() int {
        return int(C.rocksdb_writebatch_wi_count(wb.c))
}

// NewIterator returns a iterator to iterate over the records in the batch.
// This iterator does not iterate over keys in the DB. To do this use
// IteratorWithBase.
func (wb *WriteBatchWithIndex) NewIterator() *WriteBatchIterator {
        data := wb.Data()
        if len(data) < 8+4 {
                return &WriteBatchIterator{}
        }
        return &WriteBatchIterator{data: data[12:]}
}

// Clear removes all the enqueued Put and Deletes.
func (wb *WriteBatchWithIndex) Clear() {
        C.rocksdb_writebatch_wi_clear(wb.c)
}

// Destroy deallocates the WriteBatchWithIndex object.
func (wb *WriteBatchWithIndex) Destroy() {
        C.rocksdb_writebatch_wi_destroy(wb.c)
        wb.c = nil
}

// NewIteratorWithBase returns an Iterator over the WriteBatchWithIndex and the DB.
// baseIter should be an Iterator over the DB.
func (wb *WriteBatchWithIndex) NewIteratorWithBase(baseIter *Iterator) *Iterator {
	cIter := C.rocksdb_writebatch_wi_create_iterator_with_base(wb.c, baseIter.c)
	return NewNativeIterator(unsafe.Pointer(cIter))
}

// NewIteratorWithBase returns an Iterator in a column family over the 
// WriteBatchWithIndex and the DB. baseIter should be an Iterator over the DB.
func (wb *WriteBatchWithIndex) NewIteratorWithBaseCF(baseIter *Iterator, cf *ColumnFamilyHandle) *Iterator {
        cIter := C.rocksdb_writebatch_wi_create_iterator_with_base_cf(wb.c, baseIter.c, cf.c)
        return NewNativeIterator(unsafe.Pointer(cIter))
}

// GetFromBatch returns the data associated with a key from the batch.
func (wb *WriteBatchWithIndex) GetFromBatch(opts *Options, key []byte) (*Slice, error) {
	var (
		cErr    *C.char
		cValLen C.size_t
		cKey    = byteToChar(key)
	)
	cValue := C.rocksdb_writebatch_wi_get_from_batch(wb.c, opts.c, cKey, C.size_t(len(key)), &cValLen, &cErr)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		return nil, errors.New(C.GoString(cErr))
	}
	return NewSlice(cValue, cValLen), nil
}

//GetBytesFromBatch is like GetFromBatch but returns a copy of the data.
func (wb *WriteBatchWithIndex) GetBytesFromBatch(opts *Options, key []byte) ([]byte, error) {
	var (
		cErr	*C.char
		cValLen	C.size_t
		cKey	= byteToChar(key)
	)
	cValue := C.rocksdb_writebatch_wi_get_from_batch(wb.c, opts.c, cKey, C.size_t(len(key)), &cValLen, &cErr)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		return nil, errors.New(C.GoString(cErr))
	}
	if cValue == nil {
		return nil, nil
	}
	defer C.free(unsafe.Pointer(cValue))
	return C.GoBytes(unsafe.Pointer(cValue), C.int(cValLen)), nil
}

// GetFromBatchCF returns the data associated with a key from the batch in a column 
// family.
func (wb *WriteBatchWithIndex) GetFromBatchCF(opts *Options, cf *ColumnFamilyHandle, key []byte) (*Slice, error) {
        var (
                cErr    *C.char
                cValLen C.size_t
                cKey    = byteToChar(key)
        )
        cValue := C.rocksdb_writebatch_wi_get_from_batch_cf(wb.c, opts.c, cf.c, cKey, C.size_t(len(key)), &cValLen, &cErr)
        if cErr != nil {
                defer C.free(unsafe.Pointer(cErr))
                return nil, errors.New(C.GoString(cErr))
        }
        return NewSlice(cValue, cValLen), nil
}

//GetBytesFromBatchCF is like GetFromBatchCF but returns a copy of the data.
func (wb *WriteBatchWithIndex) GetBytesFromBatchCF(opts *Options, cf *ColumnFamilyHandle, key []byte) ([]byte, error) {
        var (
                cErr    *C.char
                cValLen C.size_t
                cKey    = byteToChar(key)
        )
        cValue := C.rocksdb_writebatch_wi_get_from_batch_cf(wb.c, opts.c, cf.c, cKey, C.size_t(len(key)), &cValLen, &cErr)
        if cErr != nil {
                defer C.free(unsafe.Pointer(cErr))
                return nil, errors.New(C.GoString(cErr))
        }
        if cValue == nil {
                return nil, nil
        }
        defer C.free(unsafe.Pointer(cValue))
        return C.GoBytes(unsafe.Pointer(cValue), C.int(cValLen)), nil
}

// GetFromBatchAndDB returns the data associated with a key from the batch 
// and the DB.
func (wb *WriteBatchWithIndex) GetFromBatchAndDB(db *DB, opts *ReadOptions, key []byte) (*Slice, error) {
	var (
		cErr	*C.char
		cValLen	C.size_t
		cKey	= byteToChar(key)
	)
	cValue := C.rocksdb_writebatch_wi_get_from_batch_and_db(wb.c, db.c, opts.c, cKey, C.size_t(len(key)), &cValLen, &cErr)
        if cErr != nil {
                defer C.free(unsafe.Pointer(cErr))
                return nil, errors.New(C.GoString(cErr))
        }
        return NewSlice(cValue, cValLen), nil
}

// GetBytesFromBatchAndDB is like GetFromBatchAndDB but returns a copy of the data.
func (wb *WriteBatchWithIndex) GetBytesFromBatchAndDB(db *DB, opts *ReadOptions, key []byte) ([]byte, error) {
        var (
                cErr    *C.char
                cValLen C.size_t
                cKey    = byteToChar(key)
        )
        cValue := C.rocksdb_writebatch_wi_get_from_batch_and_db(wb.c, db.c, opts.c, cKey, C.size_t(len(key)), &cValLen, &cErr)
        if cErr != nil {
                defer C.free(unsafe.Pointer(cErr))
                return nil, errors.New(C.GoString(cErr))
        }
        if cValue == nil {
                return nil, nil
        }
        defer C.free(unsafe.Pointer(cValue))
        return C.GoBytes(unsafe.Pointer(cValue), C.int(cValLen)), nil
}

// GetFromBatchAndDBCF returns the data associated with a key from the batch 
// and the DB in a column family.
func (wb *WriteBatchWithIndex) GetFromBatchAndDBCF(db *DB, opts *ReadOptions, cf *ColumnFamilyHandle, key []byte) (*Slice, error) {
        var (
                cErr    *C.char
                cValLen C.size_t
                cKey    = byteToChar(key)
        )
        cValue := C.rocksdb_writebatch_wi_get_from_batch_and_db_cf(wb.c, db.c, opts.c, cf.c, cKey, C.size_t(len(key)), &cValLen, &cErr)
        if cErr != nil {
                defer C.free(unsafe.Pointer(cErr))
                return nil, errors.New(C.GoString(cErr))
        }
        return NewSlice(cValue, cValLen), nil
}

// GetBytesFromBatchAndDBCF is like GetFromBatchAndDBCF but returns a copy of 
// the data.
func (wb *WriteBatchWithIndex) GetBytesFromBatchAndDBCF(db *DB, opts *ReadOptions, cf *ColumnFamilyHandle, key []byte) ([]byte, error) {
        var (
                cErr    *C.char
                cValLen C.size_t
                cKey    = byteToChar(key)
        )
        cValue := C.rocksdb_writebatch_wi_get_from_batch_and_db_cf(wb.c, db.c, opts.c, cf.c, cKey, C.size_t(len(key)), &cValLen, &cErr)
        if cErr != nil {
                defer C.free(unsafe.Pointer(cErr))
                return nil, errors.New(C.GoString(cErr))
        }
        if cValue == nil {
                return nil, nil
        }
        defer C.free(unsafe.Pointer(cValue))
        return C.GoBytes(unsafe.Pointer(cValue), C.int(cValLen)), nil
}
