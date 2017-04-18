package gorocksdb

import (
	"io/ioutil"
	"testing"

	"github.com/facebookgo/ensure"
)

func TestOpenTxnDb(t *testing.T) {
	txn_db := newTestTxnDB(t, "TestOpenTxnDb", nil)
	defer txn_db.TxnDBClose()
}

func TestTxnDBCRUDAndCommitRollback(t *testing.T) {
	txn_db := newTestTxnDB(t, "TestTxnDBGet", nil)
	defer txn_db.TxnDBClose()

	var (
		givenKey1 = []byte("hello")
		givenKey2 = []byte("foo")
		givenVal1 = []byte("world1")
		givenVal2 = []byte("world2")
		givenVal3 = []byte("bar")
		wo	  = NewDefaultWriteOptions()
		ro	  = NewDefaultReadOptions()
		to	  = NewDefaultTxnOptions()
		txn	  = txn_db.Begin(wo, to, nil)
	)

	// create
	ensure.Nil(t, txn.TxnPut(givenKey1, givenVal1))
	ensure.Nil(t, txn.TxnPut(givenKey2, givenVal3))

	// retrieve
	v1, err := txn.TxnGet(ro, givenKey1)
	defer v1.Free()
	ensure.Nil(t, err)
	ensure.DeepEqual(t, v1.Data(), givenVal1)

	// update
	ensure.Nil(t, txn.TxnPut(givenKey1, givenVal2))
	v2, err := txn.TxnGet(ro, givenKey1)
	defer v2.Free()
	ensure.Nil(t, err)
	ensure.DeepEqual(t, v2.Data(), givenVal2)

/*	// delete
	ensure.Nil(t, txn.TxnDelete(givenKey1))
	v3, err := txn.TxnGet(ro, givenKey1)
	ensure.Nil(t, err)
	ensure.True(t, v3.Data() == nil)
*/

	// commit transaction
	ensure.Nil(t, txn.Commit())
	// retrieve outside of the transaction
	v4, err := txn_db.TxnDBGet(ro, givenKey2)
	defer v4.Free()
	ensure.Nil(t, err)
	ensure.DeepEqual(t, v4.Data(), givenVal3)

	// rollback
	txn2 := txn_db.Begin(wo, to, txn)
	ensure.Nil(t, txn.TxnPut(givenKey2, givenVal1))
	ensure.Nil(t, txn2.Rollback())
	v5, err := txn_db.TxnDBGet(ro, givenKey2)
	defer v5.Free()
	ensure.Nil(t, err)
	ensure.DeepEqual(t, v5.Data(), givenVal3)

	// is this ok? assign nil? DestroyDB()?
	//clean up
	//txn_db.txn_opts.Destroy()
	//to.Destroy()
	//wo.Destroy()
	//ro.Destroy()
	//ensure.Nil(t, DestroyDb(txn_db.Name(), txn_db.opts))
}



// test iterate

func newTestTxnDB(t *testing.T, name string, applyOpts func(opts *Options, txn_db_opts *TxnDBOptions)) *TxnDB {
	dir, err := ioutil.TempDir("", "gorocksdb-"+name)
	ensure.Nil(t, err)

	opts := NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	txn_db_opts := NewDefaultTxnDBOptions()
	if applyOpts != nil {
		applyOpts(opts, txn_db_opts)
	}
	txn_db, err := OpenTxnDb(opts, txn_db_opts, dir)
	ensure.Nil(t, err)

	return txn_db

}
