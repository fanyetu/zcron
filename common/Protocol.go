package common

import (
	"context"
	"encoding/json"
	"github.com/gorhill/cronexpr"
	"strings"
	"time"
)

type JobEventType int64

const (
	SAVE   JobEventType = 1
	DELETE JobEventType = 2
	KILL   JobEventType = 3
)

type Job struct {
	JobName  string `json:"jobName"`
	Command  string `json:"command"`
	CronExpr string `json:"cronExpr"`
}

type JobEvent struct {
	Job  *Job
	Type JobEventType
}

type JobPlan struct {
	Job      *Job                 // 任务实体
	Expr     *cronexpr.Expression // cron解析结果
	NextTime time.Time            // 下次执行时间
}

type JobExecutingInfo struct {
	Job      *Job
	PlanTime time.Time
	RealTime time.Time

	CancelCtx  context.Context
	CancelFunc context.CancelFunc
}

type JobExecuteResult struct {
	ExecutingInfo *JobExecutingInfo // 执行状态
	Output        []byte
	Err           error
	StartTime     time.Time
	EndTime       time.Time
}

type Response struct {
	Errno int         `json:"errno"`
	Msg   string      `json:"msg"`
	Data  interface{} `json:"data"`
}

const SUCCESS int = 0
const FAILURE int = 0

func BuildJobExecutionInfo(jobPlan *JobPlan) (jobExecutionInfo *JobExecutingInfo) {
	// 创建执行信息的时候，创建一个可以被取消的上下文
	var (
		cancelCtx  context.Context
		cancelFunc context.CancelFunc
	)

	cancelCtx, cancelFunc = context.WithCancel(context.Background())
	jobExecutionInfo = &JobExecutingInfo{
		Job:      jobPlan.Job,
		PlanTime: jobPlan.NextTime,
		RealTime: time.Now(),

		CancelCtx:  cancelCtx,
		CancelFunc: cancelFunc,
	}
	return
}

// 构建任务计划
func BuildJobPlan(job *Job) (jobPlan *JobPlan, err error) {
	var (
		expr *cronexpr.Expression
		now  time.Time
	)
	now = time.Now()

	if expr, err = cronexpr.Parse(job.CronExpr); err != nil {
		return
	}

	jobPlan = &JobPlan{
		Job:      job,
		Expr:     expr,
		NextTime: expr.Next(now),
	}
	return
}

func GetJobNameFromKey(key string) string {
	return strings.TrimPrefix(key, JOB_SAVE_DIR)
}

func BuildJobEvent(eventType JobEventType, job *Job) *JobEvent {
	return &JobEvent{
		Job:  job,
		Type: eventType,
	}
}

func UnpackJob(bytes []byte) (job *Job, err error) {
	var (
		jobObj *Job
	)
	jobObj = &Job{}
	if err = json.Unmarshal(bytes, jobObj); err != nil {
		return
	}

	job = jobObj
	return
}

func BuildResponse(errno int, msg string, data interface{}) (resp []byte, err error) {
	var (
		response Response
	)

	response.Errno = errno
	response.Msg = msg
	response.Data = data

	resp, err = json.Marshal(response)
	return
}
