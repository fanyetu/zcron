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
		)
		result = &common.JobExecuteResult{
			ExecutingInfo: info,
			Output:        make([]byte, 0),
		}

		// 执行开始时间
		result.StartTime = time.Now()

		// 执行命令
		cmd = exec.CommandContext(context.Background(), "C:\\cygwin64\\bin\\bash.exe", "-c", info.Job.Command)

		output, err = cmd.CombinedOutput()

		result.EndTime = time.Now()
		result.Output = output
		result.Err = err

		// 执行完成后，通知scheduler将执行信息删除
		G_scheduler.PushJobResult(result)
	}()
}

// 初始化执行器
func InitExecutor() (err error) {
	G_executor = &Executor{}
	return
}
