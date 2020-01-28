package common

import "errors"

var ERROR_LOCK_ALREADY_REQUIRED = errors.New("锁已被占用，抢锁失败")
