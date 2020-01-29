package worker

import (
	"context"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
	"zcron/common"
)

var (
	G_logSink *LogSink
)

type LogSink struct {
	client      *mongo.Client
	collection  *mongo.Collection
	logChan     chan *common.JobLog
	timeoutChan chan *common.LogBatch // 用于timer协程通知日志协程写日志
}

func (logSink *LogSink) writeLogs(batch *common.LogBatch) {
	// 插入mongodb
	logSink.collection.InsertMany(context.Background(), batch.Logs)
}

// 写日志协程
func (logSink *LogSink) writeLoop() {
	var (
		log          *common.JobLog
		batch        *common.LogBatch
		timeoutTimer time.Timer // 定时器，超过1秒提交一个批次
		timeoutBatch *common.LogBatch
	)
	for {
		select {
		case log = <-logSink.logChan:
			// 生成一个批次，批次提交
			if batch == nil {
				batch = &common.LogBatch{}
				// 初始化timer
				timeoutTimer = time.AfterFunc(time.Duration(G_config.BatchTimeout)*time.Millisecond,
					// 使用闭包，返回一个函数。这里将batch的指针传入，保证拿到的batch和外部的batch无关
					// 不直接使用外部变量
					func(timerBatch *common.LogBatch) func() {
						return func() {
							logSink.timeoutChan <- timerBatch
						}
					}(batch))
			}

			// 将日志插入到batch中
			batch.Logs = append(batch.Logs, log)

			// 如果批次的大小超过了阈值，那么进行提交
			if len(batch.Logs) >= G_config.BatchSize {
				logSink.writeLogs(batch)
				// 清空batch
				batch = nil
				// 关闭timer（关闭不一定即时）
				timeoutTimer.Stop()
			}
		case timeoutBatch = <-logSink.timeoutChan:
			// 如果超时了，那么就提交

			// 如果过期批次不是当前批次，那么说明过期批次已经提交了，跳过提交
			if timeoutBatch != batch {
				continue
			}

			logSink.writeLogs(timeoutBatch)
			batch = nil
		}
	}
}

// 新增日志
func (logSink *LogSink) AppendLog(log *common.JobLog) {
	// 通过select语法将无法写入的日志丢弃
	select {
	case logSink.logChan <- log:
	default:
		// do nothing
	}

}

// 初始化logSink
func InitLogSink() (err error) {
	var (
		logSink *LogSink
		client  *mongo.Client
		ctx     context.Context
	)

	// 连接mongodb
	if client, err = mongo.NewClient(options.Client().ApplyURI(G_config.MongoUrl)); err != nil {
		return
	}
	ctx, _ = context.WithTimeout(context.Background(), time.Duration(G_config.MongoTimeout)*time.Millisecond)
	if err = client.Connect(ctx); err != nil {
		return
	}

	logSink = &LogSink{
		client:     client,
		collection: client.Database("zcron").Collection("log"),
		logChan:    make(chan *common.JobLog, 1000),
	}

	G_logSink = logSink

	// 启动写日志协程
	go G_logSink.writeLoop()

	return
}
