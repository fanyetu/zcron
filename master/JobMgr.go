package master

import (
	"context"
	"encoding/json"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"time"
	"zcron/common"
)

type JobMgr struct {
	client *clientv3.Client
	kv     clientv3.KV
	Lease  clientv3.Lease
}

var (
	G_jobMgr *JobMgr
)

// 初始化JobMgr
func InitJobMgr() (err error) {
	var (
		config clientv3.Config
		client *clientv3.Client
		kv     clientv3.KV
		lease  clientv3.Lease
	)

	config = clientv3.Config{
		Endpoints:   G_config.EtcdEndpoints,
		DialTimeout: time.Duration(G_config.EtcdDialTimeout) * time.Second,
	}

	if client, err = clientv3.New(config); err != nil {
		return
	}

	kv = clientv3.NewKV(client)
	lease = clientv3.NewLease(client)

	G_jobMgr = &JobMgr{
		client: client,
		kv:     kv,
		Lease:  lease,
	}

	return
}

// 保存job
func (jobMgr *JobMgr) SaveJob(job *common.Job) (oldJob *common.Job, err error) {
	var (
		key          string
		value        []byte
		putResp      *clientv3.PutResponse
		oldJobResult common.Job
	)
	key = common.JOB_SAVE_DIR + job.JobName

	if value, err = json.Marshal(job); err != nil {
		return
	}

	// 存储job，并将之前的值返回
	if putResp, err = jobMgr.kv.Put(context.Background(), key, string(value), clientv3.WithPrevKV()); err != nil {
		return
	}

	// 获取之前的值
	if putResp.PrevKv != nil {
		if err = json.Unmarshal(putResp.PrevKv.Value, &oldJobResult); err != nil {
			err = nil
			return
		}

		oldJob = &oldJobResult
	}

	return
}

// 删除job
func (jobMgr *JobMgr) DeleteJob(jobName string) (oldJob *common.Job, err error) {
	var (
		key       string
		delResp   *clientv3.DeleteResponse
		oldJobObj common.Job
	)
	key = common.JOB_SAVE_DIR + jobName

	if delResp, err = jobMgr.kv.Delete(context.Background(), key, clientv3.WithPrevKV()); err != nil {
		return
	}

	if len(delResp.PrevKvs) != 0 {
		if err = json.Unmarshal(delResp.PrevKvs[0].Value, &oldJobObj); err != nil {
			err = nil
			return
		}

		oldJob = &oldJobObj
	}
	return
}

// 遍历job
func (jobMgr *JobMgr) ListJob() (jobList []*common.Job, err error) {
	var (
		jobDir  string
		getResp *clientv3.GetResponse
		kv      *mvccpb.KeyValue
		job     *common.Job
	)
	jobDir = common.JOB_SAVE_DIR

	if getResp, err = jobMgr.kv.Get(context.Background(), jobDir, clientv3.WithPrefix()); err != nil {
		return
	}

	// 开辟jobList内存空间
	jobList = make([]*common.Job, 0)

	for _, kv = range getResp.Kvs {
		job = &common.Job{}
		if err = json.Unmarshal(kv.Value, job); err != nil {
			err = nil
			continue
		}

		jobList = append(jobList, job)
	}

	return
}

// 杀死任务
func (jobMgr *JobMgr) KillJob(jobName string) (err error) {
	var (
		key       string
		leaseResp *clientv3.LeaseGrantResponse
		leaseId   clientv3.LeaseID
	)
	key = common.JOB_KILL_DIR + jobName

	// 创建一个1秒过期的租约
	if leaseResp, err = jobMgr.Lease.Grant(context.Background(), 1); err != nil {
		return
	}

	leaseId = leaseResp.ID

	if _, err = jobMgr.kv.Put(context.Background(), key, "", clientv3.WithLease(leaseId)); err != nil {
		return
	}

	return
}
