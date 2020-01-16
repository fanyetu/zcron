package master

import (
	"context"
	"encoding/json"
	"github.com/coreos/etcd/clientv3"
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
	key = "/cron/job/" + job.JobName

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
