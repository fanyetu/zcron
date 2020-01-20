package worker

import (
	"fmt"
	"time"
	"zcron/common"
)

type Scheduler struct {
	JobEventChan chan *common.JobEvent
	JobPlanTable map[string]*common.JobPlan
}

var (
	G_scheduler *Scheduler
)

// 处理任务事件
func (scheduler *Scheduler) handleJobEvent(jobEvent *common.JobEvent) {
	var (
		err     error
		jobPlan *common.JobPlan
		exists  bool
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
	}
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
			// TODO 执行任务
			fmt.Println("执行任务：", jobPlan.Job.Command)
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
		}

		// 调度一次任务，并更新timer
		scheduleAfter = scheduler.tryExecJob()
		timer.Reset(scheduleAfter)
	}
}

// 向调度协程推送事件
func (scheduler *Scheduler) PushJobEvent(jobEvent *common.JobEvent) {
	scheduler.JobEventChan <- jobEvent
}

// 初始化scheduler
func InitScheduler() (err error) {
	G_scheduler = &Scheduler{
		JobEventChan: make(chan *common.JobEvent, 1000),
		JobPlanTable: make(map[string]*common.JobPlan),
	}

	go G_scheduler.scheduleLoop()

	return
}
