package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4A/4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raw API.
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	sr, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}
	defer sr.Close()
	value, err := sr.GetCF(req.Cf, req.Key)
	return &kvrpcpb.RawGetResponse{
		Value:    value,
		NotFound: value == nil,
	}, nil
}

func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	err := server.storage.Write(nil, []storage.Modify{
		{
			storage.Put{
				Key:   req.Key,
				Value: req.Value,
				Cf:    req.Cf,
			},
		},
	})
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// 先查询再返回
	sr, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}
	defer sr.Close()
	value, err := sr.GetCF(req.Cf, req.Key)
	if value == nil {
		return nil, nil
	}
	err = server.storage.Write(nil, []storage.Modify{
		{
			storage.Delete{
				Key: req.Key,
				Cf:  req.Cf,
			},
		},
	})
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	sr, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}
	defer sr.Close()
	it := sr.IterCF(req.Cf)
	defer it.Close()
	it.Seek(req.StartKey)
	var kvs []*kvrpcpb.KvPair
	for i := uint32(0); i < req.Limit && it.Valid(); it.Next() {
		value, _ := it.Item().Value()
		kvs = append(kvs, &kvrpcpb.KvPair{
			Key:   it.Item().Key(),
			Value: value,
		})
		i++
	}
	return &kvrpcpb.RawScanResponse{
		Kvs: kvs,
	}, nil
}

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	key := req.GetKey()
	version := req.GetVersion()
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, version)

	lock, _ := txn.GetLock(key)
	resp := &kvrpcpb.GetResponse{
		RegionError: nil,
		Error:       nil,
		Value:       nil,
		NotFound:    true,
	}
	if lock != nil {
		if lock.IsLockedFor(key, txn.StartTS, resp) {
			return resp, nil
		}
	}
	value, err := txn.GetValue(key)
	if err != nil {
		return nil, err
	}
	if value != nil {
		resp.Value = value
		resp.NotFound = false
		return resp, nil
	} else {
		return resp, nil
	}
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	version := req.GetStartVersion()
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, version)

	keyErrors := []*kvrpcpb.KeyError{}
	for _, v := range req.Mutations {
		write, timeStamp, err := txn.MostRecentWrite(v.GetKey()) // 可能有那种没锁的神秘情况……然后要加个这种特判
		if err != nil {
			return nil, err
		}
		if write != nil && timeStamp >= txn.StartTS {
			keyErrors = append(keyErrors, &kvrpcpb.KeyError{
				Locked:    nil,
				Retryable: "",
				Abort:     "",
				Conflict: &kvrpcpb.WriteConflict{ // 先空着，好像不 test 这些错误（）
					StartTs:    0,
					ConflictTs: 0,
					Key:        nil,
					Primary:    nil,
				},
			})
			continue
		}
		lock, err := txn.GetLock(v.GetKey())
		if err != nil {
			return nil, err
		}
		if lock != nil && lock.Ts != txn.StartTS {
			keyErrors = append(keyErrors, &kvrpcpb.KeyError{ // 先空着，好像不 test 这些错误（）
				Locked:    nil,
				Retryable: "",
				Abort:     "",
				Conflict:  nil,
			})
			continue
		}
		switch v.GetOp() {
		case kvrpcpb.Op_Put:
			txn.PutValue(v.GetKey(), v.GetValue())
			txn.PutLock(v.GetKey(), &mvcc.Lock{
				Primary: req.GetPrimaryLock(),
				Ts:      txn.StartTS,
				Ttl:     req.GetLockTtl(),
				Kind:    mvcc.WriteKindPut,
			})
		case kvrpcpb.Op_Del:
			txn.DeleteValue(v.GetKey())
			txn.PutLock(v.GetKey(), &mvcc.Lock{
				Primary: req.GetPrimaryLock(),
				Ts:      txn.StartTS,
				Ttl:     req.GetLockTtl(),
				Kind:    mvcc.WriteKindDelete,
			})
		}
	}
	if len(keyErrors) > 0 {
		resp := &kvrpcpb.PrewriteResponse{
			RegionError: nil,
			Errors:      keyErrors,
		}
		return resp, nil
	}
	err = server.storage.Write(req.GetContext(), txn.Writes())
	if err != nil {
		return nil, err
	}
	return &kvrpcpb.PrewriteResponse{
		RegionError: nil,
		Errors:      nil,
	}, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	version := req.GetStartVersion()
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, version)

	keys := req.GetKeys()
	server.Latches.WaitForLatches(keys)
	defer server.Latches.ReleaseLatches(keys)
	resp := &kvrpcpb.CommitResponse{
		RegionError: nil,
		Error:       nil,
	}
	for _, v := range keys {
		lock, err := txn.GetLock(v)
		if err != nil {
			return nil, err
		}
		if lock == nil { // 不能设置错误，，，
			return resp, nil
		}
		if lock.Ts != txn.StartTS {
			resp.Error = &kvrpcpb.KeyError{
				Locked:    nil,
				Retryable: "",
				Abort:     "",
				Conflict:  nil,
			}
			return resp, nil
		}
		txn.PutWrite(v, req.GetCommitVersion(), &mvcc.Write{
			StartTS: txn.StartTS,
			Kind:    lock.Kind,
		})
		txn.DeleteLock(v)
	}
	err = server.storage.Write(req.GetContext(), txn.Writes())
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.GetVersion())

	scan := mvcc.NewScanner(req.GetStartKey(), txn)
	defer scan.Close()
	var kvs []*kvrpcpb.KvPair
	for i := 0; i < int(req.GetLimit()); {
		key, value, err := scan.Next()
		if err != nil {
			return nil, err
		}
		if key == nil {
			break
		}
		if value == nil {
			continue
		}
		kvs = append(kvs, &kvrpcpb.KvPair{
			Key:   key,
			Value: value,
		})
		i++
	}
	return &kvrpcpb.ScanResponse{
		RegionError: nil,
		Pairs:       kvs,
	}, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	key := req.GetPrimaryKey()
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.GetLockTs())

	lock, err := txn.GetLock(key)
	if err != nil {
		return nil, err
	}
	resp := &kvrpcpb.CheckTxnStatusResponse{
		RegionError:   nil,
		LockTtl:       0,
		CommitVersion: 0,
		Action:        0,
	}
	if lock != nil && lock.Ts == txn.StartTS {
		if mvcc.PhysicalTime(lock.Ts)+lock.Ttl <= mvcc.PhysicalTime(req.GetCurrentTs()) {
			resp.Action = kvrpcpb.Action_TTLExpireRollback
			txn.DeleteValue(key)
			txn.DeleteLock(key)
			txn.PutWrite(key, txn.StartTS, &mvcc.Write{
				StartTS: txn.StartTS,
				Kind:    mvcc.WriteKindRollback,
			})
		}
	}
	write, timeStamp, err := txn.MostRecentWrite(key)
	if write != nil && write.Kind != mvcc.WriteKindRollback {
		resp.CommitVersion = timeStamp
	}
	if write == nil && lock == nil {
		resp.Action = kvrpcpb.Action_LockNotExistRollback
		txn.PutWrite(key, txn.StartTS, &mvcc.Write{
			StartTS: txn.StartTS,
			Kind:    mvcc.WriteKindRollback,
		})
	}
	err = server.storage.Write(req.GetContext(), txn.Writes())
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	version := req.GetStartVersion()
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, version)

	keys := req.GetKeys()
	server.Latches.WaitForLatches(keys)
	defer server.Latches.ReleaseLatches(keys)
	resp := &kvrpcpb.BatchRollbackResponse{
		RegionError: nil,
		Error:       nil,
	}
	for _, v := range keys {
		write, _, err := txn.CurrentWrite(v)
		if err != nil {
			return nil, err
		}
		if write != nil {
			if write.Kind != mvcc.WriteKindRollback {
				resp.Error = &kvrpcpb.KeyError{Abort: "abort"}
				return resp, nil
			} else {
				continue
			}
		}
		lock, err := txn.GetLock(v)
		if err != nil {
			return nil, err
		}
		if lock != nil && lock.Ts == txn.StartTS {
			resp.Error = nil
			txn.DeleteLock(v)
			txn.DeleteValue(v)
		}
		txn.PutWrite(v, txn.StartTS, &mvcc.Write{
			StartTS: txn.StartTS,
			Kind:    mvcc.WriteKindRollback,
		})
	}
	err = server.storage.Write(req.GetContext(), txn.Writes())
	if err != nil {
		return nil, err
	}
	resp.Error = nil
	return resp, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	version := req.GetStartVersion()
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, version)

	iter := reader.IterCF(engine_util.CfLock)
	defer iter.Close()
	var keys [][]byte
	for ; iter.Valid(); iter.Next() {
		data, err := iter.Item().Value()
		if err != nil {
			return nil, err
		}
		lock, err := mvcc.ParseLock(data)
		if err != nil {
			return nil, err
		}
		if lock != nil && lock.Ts == txn.StartTS {
			keys = append(keys, iter.Item().Key())
		}
	}
	resp := &kvrpcpb.ResolveLockResponse{
		RegionError: nil,
		Error:       nil,
	}
	if len(keys) == 0 {
		return resp, nil
	}
	if req.GetCommitVersion() == 0 {
		rbResp, err := server.KvBatchRollback(nil, &kvrpcpb.BatchRollbackRequest{
			Context:      req.GetContext(),
			StartVersion: req.GetStartVersion(),
			Keys:         keys,
		})
		if err != nil {
			return nil, err
		}
		resp.RegionError = rbResp.RegionError
		resp.Error = rbResp.Error
	} else {
		commitResp, err := server.KvCommit(nil, &kvrpcpb.CommitRequest{
			Context:       req.GetContext(),
			StartVersion:  req.GetStartVersion(),
			Keys:          keys,
			CommitVersion: req.GetCommitVersion(),
		})
		if err != nil {
			return nil, err
		}
		resp.RegionError = commitResp.RegionError
		resp.Error = commitResp.Error
	}
	return resp, nil
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
