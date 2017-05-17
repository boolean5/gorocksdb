package gorocksdb

// #include <stdlib.h>
// #include "rocksdb/c.h"
import "C"
import (
	"errors"
	"unsafe"
)

// TxnDB is a reusable handle to a TransactionDB database on disk, 
// created by OpenTxnDb.
type TxnDB struct {
	c		*C.rocksdb_transactiondb_t
	name		 string
	opts		*Options
	txnDBOpts	*TxnDBOptions
	nilSnapshot	*TxnDBSnapshot
}

// Txn is a reusable handle to a Transaction.
type Txn struct {
	c	 *C.rocksdb_transaction_t
	opts	 *WriteOptions
	txnOpts  *TxnOptions
}

// TxnDBSnapshot provides a consistent view of read operations in a TransactionDB.
type TxnDBSnapshot struct {
	c	*C.rocksdb_snapshot_t
	cTxnDb	*C.rocksdb_transactiondb_t
}

// OpenTxnDb opens a TransactionDB database with the specified options.
func OpenTxnDb(opts *Options, txnDBOpts *TxnDBOptions, name string) (*TxnDB, error) {
	var (
		cErr	*C.char
		cName = C.CString(name)
	)
	defer C.free(unsafe.Pointer(cName))
	txnDB := C.rocksdb_transactiondb_open(opts.c, txnDBOpts.c, cName, &cErr)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		return nil, errors.New(C.GoString(cErr))
	}
	return &TxnDB{
		name: name,
		c:    txnDB,
		opts: opts,
		txnDBOpts: txnDBOpts,
		nilSnapshot: &TxnDBSnapshot{c: nil, cTxnDb: nil},
	}, nil
}

// UnsafeGetTxnDB returns the underlying c TransactionDB instance.
func (txnDB *TxnDB) UnsafeGetTxnDB() unsafe.Pointer {
	return unsafe.Pointer(txnDB.c)
}

// Name returns the name of the TransactionDB database.
func (txnDB *TxnDB) Name() string {
	return txnDB.name
}

//NewTxnDBSnapshot creates a new snapshot of the TransactionDB database.
func (txnDB *TxnDB) NewTxnDBSnapshot() *TxnDBSnapshot {
	cSnap := C.rocksdb_transactiondb_create_snapshot(txnDB.c)
	return NewTxnDBNativeSnapshot(cSnap, txnDB.c)
}

// NewTxnDBNativeSnapshot creates a TxnDBSnapshot object.
func NewTxnDBNativeSnapshot(c *C.rocksdb_snapshot_t, cTxnDb *C.rocksdb_transactiondb_t) *TxnDBSnapshot {
	return &TxnDBSnapshot{c, cTxnDb}
}

// Release removes the snapshot from the TransactionDB's list of snapshots.
func (s *TxnDBSnapshot) Release() {
	C.rocksdb_transactiondb_release_snapshot(s.cTxnDb, s.c)
	s.c, s.cTxnDb = nil, nil
}

// SetTxnDBSnapshot sets the snapshot which should be used for the read.
// The snapshot must belong to the TxnDB that is being read and must
// not have been released.
// Default: nil
func (opts *ReadOptions) SetTxnDBSnapshot(snap *TxnDBSnapshot) {
	C.rocksdb_readoptions_set_snapshot(opts.c, snap.c)
}

// NewCheckpointObject creates a new checkpoint object, used to create 
// checkpoints of the database.
func (txnDB *TxnDB) NewCheckpointObject() (*Checkpoint, error) {
        var cErr *C.char
        cCheckpoint := C.rocksdb_transactiondb_checkpoint_object_create(txnDB.c, &cErr)
        if cErr != nil {
                defer C.free(unsafe.Pointer(cErr))
                return nil, errors.New(C.GoString(cErr))
        }
        return &Checkpoint{c: cCheckpoint}, nil
}

// Begin begins and returns a rocksdb transaction.
func (txnDB *TxnDB) Begin(opts *WriteOptions, txnOpts *TxnOptions, oldTxn *Txn) *Txn {
	var cTxn  *C.rocksdb_transaction_t
	if oldTxn != nil {
		cTxn = C.rocksdb_transaction_begin(txnDB.c, opts.c, txnOpts.c, oldTxn.c)
	} else {
		cTxn = C.rocksdb_transaction_begin(txnDB.c, opts.c, txnOpts.c, nil)
	}
	return &Txn{
		c:	cTxn,
		opts:	opts,
		txnOpts: txnOpts,
	}
}

// Commit commits the rocksdb Transaction.
func (txn *Txn) Commit() error {
	var cErr	*C.char
	C.rocksdb_transaction_commit(txn.c, &cErr)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		return errors.New(C.GoString(cErr))
	}
	return nil
}

// Rollback rolls back the rocksdb Transaction.
func (txn *Txn) Rollback() error {
	var cErr	*C.char
	C.rocksdb_transaction_rollback(txn.c, &cErr)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		return errors.New(C.GoString(cErr))
	}
	return nil
}

// Destroy deallocates the rocksdb Transaction.
func (txn *Txn) Destroy() {
	C.rocksdb_transaction_destroy(txn.c)
	txn.c = nil
	txn.opts = nil
	txn.txnOpts = nil
}

// Get returns the data associated with the key, from within a transaction.
func (txn *Txn) Get(opts *ReadOptions, key []byte) (*Slice, error) {
	var (
		cErr	*C.char
		cValLen C.size_t
		cKey	= byteToChar(key)
	)
	cValue := C.rocksdb_transaction_get(txn.c, opts.c, cKey, C.size_t(len(key)), &cValLen, &cErr)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		return nil, errors.New(C.GoString(cErr))
	}
	return NewSlice(cValue, cValLen), nil
}

// GetBytes is like Get but returns a copy of the data.
func (txn *Txn) GetBytes(opts *ReadOptions, key []byte) ([]byte, error) {
	var (
		cErr	*C.char
		cValLen	C.size_t
		cKey	= byteToChar(key)
	)
	cValue := C.rocksdb_transaction_get(txn.c, opts.c, cKey, C.size_t(len(key)), &cValLen, &cErr)
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

// Get returns the data associated with the key, from outside a transaction.
func (txnDB *TxnDB) Get(opts *ReadOptions, key []byte) (*Slice, error) {
	var (
		cErr	*C.char
		cValLen C.size_t
		cKey	= byteToChar(key)
	)
	cValue := C.rocksdb_transactiondb_get(txnDB.c, opts.c, cKey, C.size_t(len(key)), &cValLen, &cErr)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		return nil, errors.New(C.GoString(cErr))
	}
	return NewSlice(cValue, cValLen), nil
}

// GetBytes is like Get but returns a copy of the data.
func (txnDB *TxnDB) GetBytes(opts *ReadOptions, key []byte) ([]byte, error) {
	var (
		cErr	*C.char
		cValLen	C.size_t
		cKey	= byteToChar(key)
	)
	cValue := C.rocksdb_transactiondb_get(txnDB.c, opts.c, cKey, C.size_t(len(key)), &cValLen, &cErr)
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

// Put writes data associated with a key to the database, within a transaction.
func (txn *Txn) Put(key, value []byte) error {
	var (
		cErr	*C.char
		cKey	= byteToChar(key)
		cValue	= byteToChar(value)
	)
	C.rocksdb_transaction_put(txn.c, cKey, C.size_t(len(key)), cValue, C.size_t(len(value)), &cErr)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		return errors.New(C.GoString(cErr))
	}
	return nil
}

// Put writes data associated with a key to the database, from outside a transaction.
func (txnDB *TxnDB) Put(opts *WriteOptions, key, value []byte) error {
		var (
			cErr	*C.char
			cKey	= byteToChar(key)
			cValue	= byteToChar(value)
		)
		C.rocksdb_transactiondb_put(txnDB.c, opts.c, cKey, C.size_t(len(key)), cValue, C.size_t(len(value)), &cErr)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		return errors.New(C.GoString(cErr))
	}
	return nil

}

// Delete deletes a key from the database, within a transaction.
func (txn *Txn) Delete(key []byte) error {
	var (
		cErr	*C.char
		cKey	= byteToChar(key)
	)
	C.rocksdb_transaction_delete(txn.c, cKey, C.size_t(len(key)), &cErr)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		return errors.New(C.GoString(cErr))
	}
	return nil

}

// Delete deletes a key from the database, from outside a transaction.
func (txnDB *TxnDB) Delete(opts *WriteOptions, key []byte) error {
	var (
		cErr	*C.char
		cKey	= byteToChar(key)
	)
	C.rocksdb_transactiondb_delete(txnDB.c, opts.c, cKey, C.size_t(len(key)), &cErr)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		return errors.New(C.GoString(cErr))
	}
	return nil
}

// NewIterator returns an Iterator over the the database inside a transaction
// that uses the ReadOptions given.
func (txn *Txn) NewIterator(opts *ReadOptions) *Iterator {
	cIter := C.rocksdb_transaction_create_iterator(txn.c, opts.c)
	return NewNativeIterator(unsafe.Pointer(cIter))
}

// Close closes the TransactionDB database.
func (txnDB *TxnDB) Close() {
	C.rocksdb_transactiondb_close(txnDB.c)
}

// To remove a TransactionDB database entirely, removing everything from the filesystem, use DestroyDb.
