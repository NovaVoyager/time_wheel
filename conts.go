package time_wheel

const (
	//时间轮大小
	secondTimeWheelSize = 60
	minuteTimeWheelSize = 60
	hourTimeWheelSize   = 24
	dayTimeWheelSize    = 365

	//时间轮时间范围
	secondTimeRange = 60
	minuteTimeRange = secondTimeRange * 60
	hourTimeRange   = minuteTimeRange * 24
	dayTimeRange    = hourTimeRange * 365
)

const (
	//时间轮类型
	secondType timeWheelType = iota + 1
	minuteType
	hourType
	dayType
)
