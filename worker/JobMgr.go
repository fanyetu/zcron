package worker

import (
	"context"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"time"
	"zcron/common"
)

// JobMgr 任务管理器
type JobMgr struct {
	client  *clientv3.Client
	kv      clientv3.KV
	lease   clientv3.Lease
	watcher clientv3.Watcher
}

var (
	G_jobMgr *JobMgr
)

// 创建一个分布式锁
func (jobMgr *JobMgr) CreateJobLock(jobName string) (jobLock *JobLock) {
	jobLock = &JobLock{
		kv:      jobMgr.kv,
		lease:   jobLock.lease,
		jobName: jobName,
	}
	return
}

// 监听任务变化，并传送给scheduler
func (jobMgr *JobMgr) watchJob() (err error) {
	var (
		getResp       *clientv3.GetResponse
		kvpair        *mvccpb.KeyValue
		job           *common.Job
		jobEvent      *common.JobEvent
		watchRevision int64
		watchChan     clientv3.WatchChan
		watchResp     clientv3.WatchResponse
		event         *clientv3.Event
		jobName       string
	)

	// 获取当前所有的job，并拿到当前集群的revision
	if getResp, err = jobMgr.kv.Get(context.Background(), common.JOB_SAVE_DIR, clientv3.WithPrefix()); err != nil {
		return
	}

	// 将所有的job投递到scheduler
	for _, kvpair = range getResp.Kvs {
		if job, err = common.UnpackJob(kvpair.Value); err == nil {
			// 构建事件
			jobEvent = common.BuildJobEvent(common.SAVE, job)

			G_scheduler.PushJobEvent(jobEvent)
		}
	}

	// 启动协程监听job变化
	go func() {
		watchRevision = getResp.Header.Revision + 1
		// 监听所有job的变化
		watchChan = jobMgr.watcher.Watch(context.Background(), common.JOB_SAVE_DIR, clientv3.WithPrefix(), clientv3.WithRev(watchRevision))

		for watchResp = range watchChan {
			for _, event = range watchResp.Events {
				switch event.Type {
				case mvccpb.PUT:
					if job, err = common.UnpackJob(event.Kv.Value); err != nil {
						continue
					}

					jobEvent = common.BuildJobEvent(common.SAVE, job)
				case mvccpb.DELETE:
					jobName = common.GetJobNameFromKey(string(event.Kv.Key))
					jobEvent = common.BuildJobEvent(common.DELETE, &common.Job{
						JobName: jobName,
					})
				}
				G_scheduler.PushJobEvent(jobEvent)
			}
		}
	}()

	return
}

// 监听杀死任务通知
func (jobMgr *JobMgr) watchKiller() (err error) {
	var (
		watchChan clientv3.WatchChan
		watchResp clientv3.WatchResponse
		event     *clientv3.Event
		job       *common.Job
		jobEvent  *common.JobEvent
	)
	// 启动协程，监听killer目录的变化
	go func() {
		// 从当前时刻开始监听
		watchChan = jobMgr.watcher.Watch(context.Background(), common.JOB_KILL_DIR, clientv3.WithPrefix())
		for watchResp = range watchChan {
			for _, event = range watchResp.Events {
				switch event.Type {
				case mvccpb.PUT:
					// 只关心put类型的事件
					job = &common.Job{
						JobName: string(event.Kv.Key),
					}
					jobEvent = common.BuildJobEvent(common.KILL, job)
					// 推送事件到scheduler
					G_scheduler.PushJobEvent(jobEvent)
				case mvccpb.DELETE:
				}
			}
		}
	}()
	return
}

// 初始化JobMgr
func InitJobMgr() (err error) {
	var (
		config  clientv3.Config
		client  *clientv3.Client
		kv      clientv3.KV
		lease   clientv3.Lease
		watcher clientv3.Watcher
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
	watcher = clientv3.NewWatcher(client)

	G_jobMgr = &JobMgr{
		client:  client,
		kv:      kv,
		lease:   lease,
		watcher: watcher,
	}

	if err = G_jobMgr.watchJob(); err != nil {
		return
	}

	if err = G_jobMgr.watchKiller(); err != nil {
		return
	}

	return
}
