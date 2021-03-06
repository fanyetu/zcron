package worker

import (
	"fmt"
	"time"
	"zcron/common"
)

// Scheduler 调度器
type Scheduler struct {
	JobEventChan      chan *common.JobEvent
	JobPlanTable      map[string]*common.JobPlan
	JobExecutingTable map[string]*common.JobExecutingInfo
	JobResultChan     chan *common.JobExecuteResult
}

var (
	G_scheduler *Scheduler
)

// 处理任务事件
func (scheduler *Scheduler) handleJobEvent(jobEvent *common.JobEvent) {
	var (
		err              error
		jobPlan          *common.JobPlan
		exists           bool
		jobExecuting     bool
		jobExecutingInfo *common.JobExecutingInfo
	)
	// 根据任务事件的类型来对当前的任务计划列表进行修改
	switch jobEvent.Type {
	case common.SAVE:
		// 如果是保存，那么就将任务更新
		if jobPlan, err = common.BuildJobPlan(jobEvent.Job); err != nil {
			return
		}

		scheduler.JobPlanTable[jobEvent.Job.JobName] = jobPlan
	case common.DELETE:
		// 如果任务在表中，就删除
		if jobPlan, exists = scheduler.JobPlanTable[jobEvent.Job.JobName]; exists {
			delete(scheduler.JobPlanTable, jobEvent.Job.JobName)
		}
	case common.KILL:
		// 强杀任务事件
		if jobExecutingInfo, jobExecuting = scheduler.JobExecutingTable[jobEvent.Job.JobName]; jobExecuting {
			// 调用任务执行信息中的cancelFunc，取消任务执行
			jobExecutingInfo.CancelFunc()
		}
	}
}

// 处理任务执行结果
func (scheduler *Scheduler) handleJobResult(jobResult *common.JobExecuteResult) {
	var (
		jobLog *common.JobLog
	)
	// 将执行信息从table中删除
	delete(scheduler.JobExecutingTable, jobResult.ExecutingInfo.Job.JobName)

	// 打印执行结果
	fmt.Println("执行结果：", string(jobResult.Output), jobResult.Err, jobResult.StartTime, jobResult.EndTime)

	// 将日志写入到mongodb中
	if jobResult.Err != common.ERROR_LOCK_ALREADY_REQUIRED {
		// 构建JobLog
		jobLog = &common.JobLog{
			JobName:      jobResult.ExecutingInfo.Job.JobName,
			Command:      jobResult.ExecutingInfo.Job.Command,
			PlanTime:     jobResult.ExecutingInfo.PlanTime.UnixNano() / 1000 / 1000,
			ScheduleTime: jobResult.ExecutingInfo.RealTime.UnixNano() / 1000 / 1000,
			StartTime:    jobResult.StartTime.UnixNano() / 1000 / 1000,
			EndTime:      jobResult.EndTime.UnixNano() / 1000 / 1000,
			Output:       string(jobResult.Output),
		}

		if jobResult.Err != nil {
			jobLog.Err = jobResult.Err.Error()
		} else {
			jobLog.Err = ""
		}

		// 写入日志
		G_logSink.AppendLog(jobLog)
	}
}

func (scheduler *Scheduler) tryStartJob(jobPlan *common.JobPlan) {
	var (
		executingInfo *common.JobExecutingInfo
		jobExecuting  bool
	)
	// 判断当前是否在执行队列中
	if executingInfo, jobExecuting = scheduler.JobExecutingTable[jobPlan.Job.JobName]; jobExecuting {
		fmt.Println("当前任务未退出，取消执行：", jobPlan.Job.JobName)
		return
	}

	// 构建执行信息
	executingInfo = common.BuildJobExecutionInfo(jobPlan)

	// 保存执行状态
	scheduler.JobExecutingTable[jobPlan.Job.JobName] = executingInfo

	// 执行任务
	G_executor.ExecuteJob(executingInfo)
}

// 尝试执行planTable中的任务
func (scheduler *Scheduler) tryExecJob() (scheduleAfter time.Duration) {
	var (
		jobPlan  *common.JobPlan
		now      time.Time
		nearTime *time.Time
	)

	scheduleAfter = 1 * time.Second

	// 如果当前table为空，那么直接退出
	if len(scheduler.JobPlanTable) == 0 {
		return
	}

	now = time.Now()

	// 循环当前的table
	for _, jobPlan = range scheduler.JobPlanTable {
		// 如果下次执行时间小于等于当前时间，那么就执行任务
		if jobPlan.NextTime.Before(now) || jobPlan.NextTime.Equal(now) {
			scheduler.tryStartJob(jobPlan)
			// 执行完成后更新下次执行时间
			jobPlan.NextTime = jobPlan.Expr.Next(now)
		}

		// 统计最近一个要过期的任务
		if nearTime == nil || jobPlan.NextTime.Before(*nearTime) {
			nearTime = &jobPlan.NextTime
		}
	}

	// 下次执行调度的间隔
	if nearTime != nil {
		scheduleAfter = (*nearTime).Sub(now)
	}
	return
}

// 调度协程
func (scheduler *Scheduler) scheduleLoop() {
	var (
		jobEvent      *common.JobEvent
		scheduleAfter time.Duration
		timer         *time.Timer
		jobResult     *common.JobExecuteResult
	)

	// 先尝试执行一次任务
	scheduleAfter = scheduler.tryExecJob()

	// 然后设置定时器，after之后，再执行一次任务
	timer = time.NewTimer(scheduleAfter)

	for {
		// 循环监听event事件
		select {
		case jobEvent = <-scheduler.JobEventChan:
			// 处理任务事件
			scheduler.handleJobEvent(jobEvent)
		case <-timer.C: // 最近的任务到期了
		case jobResult = <-scheduler.JobResultChan:
			scheduler.handleJobResult(jobResult)
		}

		// 调度一次任务，并更新timer
		scheduleAfter = scheduler.tryExecJob()
		timer.Reset(scheduleAfter)
	}
}

// PushJobEvent 向调度协程推送事件
func (scheduler *Scheduler) PushJobEvent(jobEvent *common.JobEvent) {
	scheduler.JobEventChan <- jobEvent
}

func (scheduler *Scheduler) PushJobResult(jobResult *common.JobExecuteResult) {
	scheduler.JobResultChan <- jobResult
}

// InitScheduler 初始化scheduler
func InitScheduler() (err error) {
	G_scheduler = &Scheduler{
		JobEventChan:      make(chan *common.JobEvent, 1000),
		JobPlanTable:      make(map[string]*common.JobPlan),
		JobExecutingTable: make(map[string]*common.JobExecutingInfo),
		JobResultChan:     make(chan *common.JobExecuteResult, 1000),
	}

	go G_scheduler.scheduleLoop()

	return
}
