package worker

import (
	"context"
	"os/exec"
	"time"
	"zcron/common"
)

// Executor 执行器
type Executor struct {
}

var (
	G_executor *Executor
)

func (executor *Executor) ExecuteJob(info *common.JobExecutingInfo) {
	go func() {
		var (
			cmd    *exec.Cmd
			output []byte
			err    error
			result *common.JobExecuteResult
			lock   *JobLock
		)
		result = &common.JobExecuteResult{
			ExecutingInfo: info,
			Output:        make([]byte, 0),
		}

		// 首先创建分布式锁
		lock = G_jobMgr.CreateJobLock(info.Job.JobName)

		// 执行开始时间
		result.StartTime = time.Now()

		// 尝试上锁
		err = lock.TryLock()
		defer lock.Unlock()

		if err != nil {
			// 上锁失败
			result.Err = err
			result.EndTime = time.Now()
		} else {
			result.StartTime = time.Now()
			// 执行命令
			cmd = exec.CommandContext(context.Background(), "C:\\cygwin64\\bin\\bash.exe", "-c", info.Job.Command)

			output, err = cmd.CombinedOutput()

			result.EndTime = time.Now()
			result.Output = output
			result.Err = err
		}

		// 执行完成后，通知scheduler将执行信息删除
		G_scheduler.PushJobResult(result)
	}()
}

// 初始化执行器
func InitExecutor() (err error) {
	G_executor = &Executor{}
	return
}
