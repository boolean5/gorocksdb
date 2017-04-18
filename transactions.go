package gorocksdb

// #include <stdlib.h>
// #include "rocksdb/c.h"
import "C"
import (
	"errors"
	"unsafe"
)

// TxnDB is a reusable handle to a TransactionDB database on disk, created by OpenTxnDb.
type TxnDB struct {
	c	 *C.rocksdb_transactiondb_t
	name	 string
	opts	 *Options
	txn_db_opts *TxnDBOptions
}

// Txn is a reusable handle to a Transaction
type Txn struct {
	c	 *C.rocksdb_transaction_t
	opts	 *WriteOptions
	txn_opts *TxnOptions
}

// OpenTxnDb opens a TransactionDB database with the specified options.
func OpenTxnDb(opts *Options, txn_db_opts *TxnDBOptions, name string) (*TxnDB, error) {
	var (
		cErr	*C.char
		cName = C.CString(name)
	)
	defer C.free(unsafe.Pointer(cName))
	txn_db := C.rocksdb_transactiondb_open(opts.c, txn_db_opts.c, cName, &cErr)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		return nil, errors.New(C.GoString(cErr))
	}
	return &TxnDB{
		name: name,
		c:    txn_db,
		opts: opts,
		txn_db_opts: txn_db_opts,
	}, nil
}

// check if the next 2 funcs are used and if not delete them
// UnsafeGetTxnDB returns the underlying c TransactionDB instance.
func (txn_db *TxnDB) UnsafeGetTxnDB() unsafe.Pointer {
	return unsafe.Pointer(txn_db.c)
}

// TxnDBName returns the name of the TransactionDB database.
func (txn_db *TxnDB) TxnDBName() string {
	return txn_db.name
}

// Begin begins and returns a rocksdb transaction
func (txn_db *TxnDB) Begin(opts *WriteOptions, txn_opts *TxnOptions, old_txn *Txn) *Txn {
	var cTxn  *C.rocksdb_transaction_t
	if old_txn != nil {
		cTxn = C.rocksdb_transaction_begin(txn_db.c, opts.c, txn_opts.c, old_txn.c)
	} else {
		cTxn = C.rocksdb_transaction_begin(txn_db.c, opts.c, txn_opts.c, nil)
	}
	return &Txn{
		c:	cTxn,
		opts:	opts,
		txn_opts: txn_opts,
	}
}

// TxnGet returns the data associated with the key, from within a transaction.
func (txn *Txn) TxnGet(opts *ReadOptions, key []byte) (*Slice, error) {
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

// TxnGetBytes is like Get but returns a copy of the data.
func (txn *Txn) TxnGetBytes(opts *ReadOptions, key []byte) ([]byte, error) {
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

// TxnDBGet returns the data associated with the key, from outside a transaction.
func (txn_db *TxnDB) TxnDBGet(opts *ReadOptions, key []byte) (*Slice, error) {
	var (
		cErr	*C.char
		cValLen C.size_t
		cKey	= byteToChar(key)
	)
	cValue := C.rocksdb_transactiondb_get(txn_db.c, opts.c, cKey, C.size_t(len(key)), &cValLen, &cErr)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		return nil, errors.New(C.GoString(cErr))
	}
	return NewSlice(cValue, cValLen), nil
}
// TxnDBGetBytes is like Get but returns a copy of the data.
func (txn_db *Txn) TxnDBGetBytes(opts *ReadOptions, key []byte) ([]byte, error) {
	var (
		cErr	*C.char
		cValLen	C.size_t
		cKey	= byteToChar(key)
	)
	cValue := C.rocksdb_transactiondb_get(txn_db.c, opts.c, cKey, C.size_t(len(key)), &cValLen, &cErr)
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

// TxnPut writes data associated with a key to the database, within a transaction
func (txn *Txn) TxnPut(key, value []byte) error {
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

// Delete

// Iterate

// Commit commits the rocksdb Transaction
func (txn *Txn) Commit() error {
	var cErr	*C.char
	C.rocksdb_transaction_commit(txn.c, &cErr)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		return errors.New(C.GoString(cErr))
	}
	return nil
}

// Rollback rollsback the rocksdb Transaction
func (txn *Txn) Rollback() error {
	var cErr	*C.char
	C.rocksdb_transaction_rollback(txn.c, &cErr)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		return errors.New(C.GoString(cErr))
	}
	return nil
}

// TxnDBClose closes the TransactionDB database
func (txn_db *TxnDB) TxnDBClose() {
	C.rocksdb_transactiondb_close(txn_db.c)
}

// To remove a TransactionDB database entirely, removing everything from the filesystem, use DestroyDb
