package gorocksdb

// #include "rocksdb/c.h"
import "C"

// TxnDBOptions represent the available options when opening a TransactionDB database
type TxnDBOptions struct {
	c  *C.rocksdb_transactiondb_options_t
}

// NewDefaultTxnDBOptions creates the default TransactionDB options.
func NewDefaultTxnDBOptions() *TxnDBOptions {
	return NewNativeTxnDBOptions(C.rocksdb_transactiondb_options_create())
}

// NewNativeTxnDBOptions creates a TxnDBOptions object.
func NewNativeTxnDBOptions(c *C.rocksdb_transactiondb_options_t) *TxnDBOptions {
	return &TxnDBOptions{c}
}

// SetMaxNumLocks specifies the maximum number of keys that can be locked at the same
// time per column family.
// If the number of locked keys is greater than max_num_locks, transaction
// writes will return an error.
// If this value is not positive, no limit will be enforced.
// Default: -1
func (txn_db_opts  *TxnDBOptions) SetMaxNumLocks(value int64) {
	C.rocksdb_transactiondb_options_set_max_num_locks(txn_db_opts.c, C.int64_t(value))
}

// SetNumStripes sets the concurrency by dividing the lock table (per column family) 
// into more sub-tables, each with their own separate mutex.
// Increasing it increases the concurrency
// Default: 16
func (txn_db_opts *TxnDBOptions) SetNumStripes(value int) {
	C.rocksdb_transactiondb_options_set_num_stripes(txn_db_opts.c, C.size_t(value))
}

// SetTransactionLockTimeout if the value is positive, specifies the default 
// wait timeout in milliseconds when a transaction attempts to lock a key 
// if not specified by TxnOpts.SetLockTimeout().
// If 0, no waiting is done if a lock cannot instantly be acquired.
// If negative, there is no timeout.  Not using a timeout is not recommended
// as it can lead to deadlocks.  Currently, there is no deadlock-detection to
// recover from a deadlock.
// Default: 1000
func (txn_db_opts *TxnDBOptions) SetTransactionLockTimeout(value int64) {
	C.rocksdb_transactiondb_options_set_transaction_lock_timeout(txn_db_opts.c, C.int64_t(value))
}

// SetDefaultLockTimeout if the value is positive, specifies the wait timeout 
// in milliseconds when writing a key OUTSIDE of a transaction 
// (ie by calling DB::Put(),Merge(),Delete(),Write() directly).
// If 0, no waiting is done if a lock cannot instantly be acquired.
// If negative, there is no timeout and will block indefinitely when acquiring
// a lock.
//
// Not using a timeout can lead to deadlocks.  Currently, there
// is no deadlock-detection to recover from a deadlock.  While DB writes
// cannot deadlock with other DB writes, they can deadlock with a transaction.
// A negative timeout should only be used if all transactions have a small
// expiration set.
// Default: 1000
func (txn_db_opts *TxnDBOptions) SetDefaultLockTimeout(value int64) {
	C.rocksdb_transactiondb_options_set_default_lock_timeout(txn_db_opts.c, C.int64_t(value))
}

// Destroy deallocates the TxnDBOptions object.
func (txn_db_opts *TxnDBOptions) Destroy() {
	C.rocksdb_transactiondb_options_destroy(txn_db_opts.c)
	txn_db_opts.c = nil
}

// TxnOptions represent the available options when starting a Transaction.
type TxnOptions struct {
	c  *C.rocksdb_transaction_options_t
}

// NewDefaultTxnOptions creates the default Transaction options.
func NewDefaultTxnOptions() *TxnOptions {
	return NewNativeTxnOptions(C.rocksdb_transaction_options_create())
}

// NewNativeTxnOptions creates a TxnOptions object.
func NewNativeTxnOptions(c *C.rocksdb_transaction_options_t) *TxnOptions {
	return &TxnOptions{c}
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
func (txn_opts *TxnOptions) SetSnapshot(value bool) {
	C.rocksdb_transaction_options_set_set_snapshot(txn_opts.c, boolToChar(value))
}

// find the equivalent of Status::Busy in the go wrapper and change this comment

// SetDeadlockDetect when the value is true, means that before acquiring locks, 
// this transaction will check if doing so will cause a deadlock. 
// If so, it will return with Status::Busy. The user should retry their transaction.
// Default: false
func (txn_opts *TxnOptions) SetDeadlockDetect(value bool) {
	C.rocksdb_transaction_options_set_deadlock_detect(txn_opts.c, boolToChar(value))
}

// SetLockTimeout if the value is positive, specifies the wait timeout in 
// milliseconds when a transaction attempts to lock a key.
//
// If 0, no waiting is done if a lock cannot instantly be acquired.
// If negative, the value set by TxnBOptions.SetTransactionLockTimeout()
//  will be used.
// Default: -1
func (txn_opts *TxnOptions) SetLockTimeout(value int64) {
	C.rocksdb_transaction_options_set_lock_timeout(txn_opts.c, C.int64_t(value))
}

// SetExpiration sets the Expiration duration in milliseconds.  
// If non-negative, transactions that last longer than this many milliseconds 
// will fail to commit.  If not set, a forgotten transaction that is never committed,
// rolled back, or deleted will never relinquish any locks it holds.  
// This could prevent keys from being written by other writers.
// Default: -1
func (txn_opts *TxnOptions) SetExpiration(value int64) {
	C.rocksdb_transaction_options_set_expiration(txn_opts.c, C.int64_t(value))
}

// SetDeadlockDetectdepth sets the number of traversals to make during deadlock 
// detection.
// Default: 50
func (txn_opts *TxnOptions) SetDeadlockDetectDepth(value int64) {
	C.rocksdb_transaction_options_set_deadlock_detect_depth(txn_opts.c, C.int64_t(value))
}

/*
// SetMaxWriteBatchSize sets the maximum number of bytes used for the write batch.
// 0 means no limit.
// Default: 0
func (txn_opts *TxnOptions) SetMaxWriteBatchSize(value int) {
	C.rocksdb_transaction_options_set_max_write_batch_size(txn_opts.c, C.size_t(value))
}
*/

// Destroy deallocates the TxnOptions object.
func (txn_opts *TxnOptions) Destroy() {
	C.rocksdb_transaction_options_destroy(txn_opts.c)
	txn_opts.c = nil
}
