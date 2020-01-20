package common

import (
	"encoding/json"
	"strings"
)

type JobEventType int64

const (
	SAVE   JobEventType = 1
	DELETE JobEventType = 2
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

type Response struct {
	Errno int         `json:"errno"`
	Msg   string      `json:"msg"`
	Data  interface{} `json:"data"`
}

const SUCCESS int = 0
const FAILURE int = 0

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
