package time_wheel

// timeWheelType 时间轮类型
type timeWheelType int8

// TaskFun 时间轮任务函数
//待执行的任务
type TaskFun func()
