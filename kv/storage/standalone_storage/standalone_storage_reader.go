package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

type StandaloneStorageReader struct {
	txn *badger.Txn
}

func (sr *StandaloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, _ := engine_util.GetCFFromTxn(sr.txn, cf, key) // 没有也不返回错误
	return val, nil
}

func (sr *StandaloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, sr.txn)
}

func (sr *StandaloneStorageReader) Close() {
	_ = sr.txn.Commit()
	sr.txn.Discard()
}
