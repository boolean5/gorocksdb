package gorocksdb

import (
	"io/ioutil"
	"testing"
	"github.com/facebookgo/ensure"
)

func TestOpenTxnDb(t *testing.T) {
	txnDB := newTestTxnDB(t, "TestOpenTxnDb", nil)
	defer txnDB.Close()
}

func TestTxnDBCRUDAndCommitRollback(t *testing.T) {
	txnDB := newTestTxnDB(t, "TestTxnDBGet", nil)
	defer txnDB.Close()

	var (
		givenKey1 = []byte("hello")
		givenKey2 = []byte("foo")
		givenKey3 = []byte("foo2")
		givenVal1 = []byte("world1")
		givenVal2 = []byte("world2")
		givenVal3 = []byte("bar")
		givenVal4 = []byte("bar2")
		wo	  = NewDefaultWriteOptions()
		ro	  = NewDefaultReadOptions()
		to	  = NewDefaultTxnOptions()
		txn	  = txnDB.Begin(wo, to, nil)
	)

	// create
	ensure.Nil(t, txn.Put(givenKey1, givenVal1))
	ensure.Nil(t, txn.Put(givenKey2, givenVal3))

	// retrieve
	v1, err := txn.Get(ro, givenKey1)
	defer v1.Free()
	ensure.Nil(t, err)
	ensure.DeepEqual(t, v1.Data(), givenVal1)

	// update
	ensure.Nil(t, txn.Put(givenKey1, givenVal2))
	v2, err := txn.Get(ro, givenKey1)
	defer v2.Free()
	ensure.Nil(t, err)
	ensure.DeepEqual(t, v2.Data(), givenVal2)

        // iterate
	iter := txn.NewIterator(ro)
	iter.SeekToFirst()
	ensure.True(t, iter.Valid())
	ensure.DeepEqual(t, iter.Key().Data(), givenKey2)
	ensure.DeepEqual(t, iter.Value().Data(), givenVal3)
	ensure.Nil(t, iter.Err())
	iter.Close()

	// delete
	ensure.Nil(t, txn.Delete(givenKey1))
	v3, err := txn.Get(ro, givenKey1)
	ensure.Nil(t, err)
	ensure.True(t, v3.Data() == nil)

	// snapshot (repeatable reads)
	ensure.Nil(t, txnDB.Put(wo, givenKey3, givenVal4))
	snap := txnDB.NewTxnDBSnapshot()
	ro.SetTxnDBSnapshot(snap)
	ensure.Nil(t, txnDB.Delete(wo, givenKey3))
	v6, err := txn.Get(ro, givenKey3)
	ensure.Nil(t, err)
	ensure.DeepEqual(t, v6.Data(), givenVal4)
	ro.SetTxnDBSnapshot(txnDB.nilSnapshot)
	snap.Release()
	v6, err = txn.Get(ro, givenKey3)
	ensure.Nil(t, err)
	ensure.True(t, v6.Data() == nil)

	// commit transaction
	ensure.Nil(t, txn.Commit())

	// retrieve outside of the transaction
	v4, err := txnDB.Get(ro, givenKey2)
	defer v4.Free()
	ensure.Nil(t, err)
	ensure.DeepEqual(t, v4.Data(), givenVal3)

	// rollback
	txn = txnDB.Begin(wo, to, txn)
	ensure.Nil(t, txn.Put(givenKey2, givenVal1))
	ensure.Nil(t, txn.Rollback())
	v5, err := txnDB.Get(ro, givenKey2)
	defer v5.Free()
	ensure.Nil(t, err)
	ensure.DeepEqual(t, v5.Data(), givenVal3)

	// deallocate transaction
	txn.Destroy()
}

func newTestTxnDB(t *testing.T, name string, applyOpts func(opts *Options, txnDBOpts *TxnDBOptions)) *TxnDB {
	dir, err := ioutil.TempDir("", "gorocksdb-"+name)
	ensure.Nil(t, err)

	opts := NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	txnDBOpts := NewDefaultTxnDBOptions()
	if applyOpts != nil {
		applyOpts(opts, txnDBOpts)
	}
	txnDB, err := OpenTxnDb(opts, txnDBOpts, dir)
	ensure.Nil(t, err)

	return txnDB
}
