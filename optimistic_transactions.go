package gorocksdb

// #include <stdlib.h>
// #include "rocksdb/c.h"
import "C"
import (
        "errors"
        "unsafe"
)

// OptimisticTxnDB is a reusable handle to an OptimisticTransactionDB database 
// on disk, created by OpenOptimisticTxnDb.
type OptimisticTxnDB struct {
        c               *C.rocksdb_optimistic_transactiondb_t
        name             string
        opts            *Options
        nilSnapshot     *Snapshot
}

func (txnDB *OptimisticTxnDB) GetBaseDB() (*DB) {
        db := C.rocksdb_get_base_db(txnDB.c)
        return &DB{c:   db,
                   name: txnDB.name,
                   opts: txnDB.opts,
        }
}

// OpenOptimisticTxnDb opens an OptimisticTransactionDB database with the 
// specified options.
func OpenOptimisticTxnDb(opts *Options, name string) (*OptimisticTxnDB, error) {
        var (
                cErr    *C.char
                cName = C.CString(name)
        )
        defer C.free(unsafe.Pointer(cName))
        txnDB := C.rocksdb_optimistic_transactiondb_open(opts.c, cName, &cErr)
        if cErr != nil {
                defer C.free(unsafe.Pointer(cErr))
                return nil, errors.New(C.GoString(cErr))
        }
        return &OptimisticTxnDB{
                name: name,
                c:    txnDB,
                opts: opts,
                nilSnapshot: &Snapshot{c: nil, cDb: nil},
        }, nil
}

// UnsafeGetOptimisticTxnDB returns the underlying c 
// OptimisticTransactionDB instance.
func (txnDB *OptimisticTxnDB) UnsafeGetOptimistcTxnDB() unsafe.Pointer {
        return unsafe.Pointer(txnDB.c)
}

// Name returns the name of the OptimisticTransactionDB database.
func (txnDB *OptimisticTxnDB) Name() string {
        return txnDB.name
}

// Begin begins and returns a rocksdb optimistic transaction.
func (txnDB *OptimisticTxnDB) Begin(opts *WriteOptions, txnOpts *OptimisticTxnOptions, oldTxn *Txn) *Txn {
        var cTxn  *C.rocksdb_transaction_t
        if oldTxn != nil {
                cTxn = C.rocksdb_optimistic_transaction_begin(txnDB.c, opts.c, txnOpts.c, oldTxn.c)
        } else {
                cTxn = C.rocksdb_optimistic_transaction_begin(txnDB.c, opts.c, txnOpts.c, nil)
        }
        return &Txn{
                c:      cTxn,
                opts:   opts,
                //txnOpts: txnOpts,
        }
}

// Close closes the OptimisticTransactionDB database.
func (txnDB *OptimisticTxnDB) Close() {
        C.rocksdb_optimistic_transactiondb_close(txnDB.c)
}

// To remove an OptimisticTransactionDB database entirely, removing everything from the filesystem, use DestroyDb.
