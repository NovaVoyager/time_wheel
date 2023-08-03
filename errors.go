package time_wheel

import "errors"

var (
	taskIdNoExistErr    = errors.New("the task id not exist")
	delayTimeOutOfRange = errors.New("the delay time out of range, max 31536000 second")
)
