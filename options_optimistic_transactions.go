package gorocksdb

// #include "rocksdb/c.h"
import "C"

// OptimisticTxnOptions represent the available options when starting an 
// Optimistic Transaction.
type OptimisticTxnOptions struct {
        c  *C.rocksdb_optimistic_transaction_options_t
}

// NewDefaultOptimisticTxnOptions creates the default Optimistic Transaction options.
func NewDefaultOptimisticTxnOptions() *OptimisticTxnOptions {
        return NewNativeOptimisticTxnOptions(C.rocksdb_optimistic_transaction_options_create())
}

// NewOptimisticNativeTxnOptions creates an OptimisticTxnOptions object.
func NewNativeOptimisticTxnOptions(c *C.rocksdb_optimistic_transaction_options_t) *OptimisticTxnOptions {
        return &OptimisticTxnOptions{c}
}

// SetSnapshot if the value is true, will ensure that any keys successfully written 
// by a transaction have not been modified outside of this transaction since the time
// the snapshot was set.
// If a snapshot has not been set, the transaction guarantees that keys have
// not been modified since the time each key was first written.
//
// Using SetSnapshot() will provide stricter isolation guarantees at the
// expense of potentially more transaction failures due to conflicts with
// other writes.
// Default: false
func (opts *OptimisticTxnOptions) SetSnapshot(value bool) {
        C.rocksdb_optimistic_transaction_options_set_set_snapshot(opts.c, boolToChar(value))
}

// Destroy deallocates the OptimisticTxnOptions object.
func (opts *OptimisticTxnOptions) Destroy() {
        C.rocksdb_optimistic_transaction_options_destroy(opts.c)
        opts.c = nil
}
