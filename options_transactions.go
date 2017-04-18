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

// add some setters

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

// add some setters

// Destroy deallocates the TxnOptions object.
func (txn_opts *TxnOptions) Destroy() {
	C.rocksdb_transaction_options_destroy(txn_opts.c)
	txn_opts.c = nil
}
