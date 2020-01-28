package worker

import (
	"context"
	"github.com/coreos/etcd/clientv3"
	"zcron/common"
)

type JobLock struct {
	kv      clientv3.KV
	lease   clientv3.Lease
	jobName string

	cancelFun context.CancelFunc
	leaseId   clientv3.LeaseID
	isLock    bool
}

// 尝试上锁
func (jobLock *JobLock) TryLock() (err error) {
	var (
		leaseResp     *clientv3.LeaseGrantResponse
		leaseId       clientv3.LeaseID
		cancelCtx     context.Context
		cancelFun     context.CancelFunc
		keepAliveChan <-chan *clientv3.LeaseKeepAliveResponse
		txn           clientv3.Txn
		lockName      string
		txnResp       *clientv3.TxnResponse
	)

	// 1.获取租约(5秒)
	if leaseResp, err = jobLock.lease.Grant(context.Background(), 5); err != nil {
		return
	}

	// 获取leaseId
	leaseId = leaseResp.ID

	// 2.自动续租
	cancelCtx, cancelFun = context.WithCancel(context.Background())
	if keepAliveChan, err = jobLock.lease.KeepAlive(cancelCtx, leaseId); err != nil {
		goto FAIL
	}

	// 3.处理自动续租的响应，如果失败，则忽略
	go func() {
		var (
			keepAliveResp *clientv3.LeaseKeepAliveResponse
		)
		for {
			select {
			case keepAliveResp = <-keepAliveChan:
				if keepAliveResp == nil {
					goto END
				}
			}
		}
	END:
	}()

	// 4.创建事务并通过事务抢锁
	txn = jobLock.kv.Txn(context.Background())
	lockName = common.JOB_LOCK_DIR + jobLock.jobName

	txn.If(clientv3.Compare(clientv3.CreateRevision(lockName), "=", 0)).
		Then(clientv3.OpPut(lockName, "", clientv3.WithLease(leaseId))).
		Else(clientv3.OpGet(lockName))

	// 提交事务
	if txnResp, err = txn.Commit(); err != nil {
		goto FAIL
	}

	// 如果事务提交失败
	if !txnResp.Succeeded {
		err = common.ERROR_LOCK_ALREADY_REQUIRED
		goto FAIL
	}

	// 如果事务调教成功，那么抢锁成功
	jobLock.cancelFun = cancelFun
	jobLock.leaseId = leaseId
	jobLock.isLock = true

	return
FAIL:
	cancelFun()                                         // 取消自动续租
	jobLock.lease.Revoke(context.Background(), leaseId) // 解除租约
	return
}

// 解锁
func (jobLock *JobLock) Unlock() {
	if jobLock.isLock {
		jobLock.cancelFun()                                         // 取消自动续租
		jobLock.lease.Revoke(context.Background(), jobLock.leaseId) // 解除租约
	}
}
