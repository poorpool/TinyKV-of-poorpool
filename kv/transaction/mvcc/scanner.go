package mvcc

import (
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"reflect"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	// Your Data Here (4C).
	key  []byte // 调用 Next() 返回的 key
	txn  *MvccTxn
	iter engine_util.DBIterator // write iter
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	return &Scanner{
		key:  startKey,
		txn:  txn,
		iter: txn.Reader.IterCF(engine_util.CfWrite),
	}
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
	scan.iter.Close()
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).
	if !scan.iter.Valid() {
		return nil, nil, nil
	}
	scan.iter.Seek(EncodeKey(scan.key, scan.txn.StartTS))
	if !scan.iter.Valid() {
		return nil, nil, nil
	}
	gotKey := DecodeUserKey(scan.iter.Item().Key())
	key := gotKey
	if !reflect.DeepEqual(gotKey, scan.key) {
		scan.key = gotKey
		return scan.Next()
	}
	for {
		scan.iter.Next()
		if !scan.iter.Valid() {
			break
		}
		gotKey = DecodeUserKey(scan.iter.Item().Key())
		if !reflect.DeepEqual(gotKey, scan.key) {
			scan.key = gotKey // 确定下一个 key
			break
		}
	}
	value, err := scan.txn.GetValue(key)
	if err != nil {
		return nil, nil, err
	}
	return key, value, err
}
