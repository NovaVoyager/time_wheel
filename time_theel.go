package time_wheel

import (
	"github.com/sony/sonyflake"
	"sync"
	"time"
)

//task 任务
type task struct {
	taskId int64

	//可以通过 round 实现1年以上的延迟时间
	round    int
	taskFunc TaskFun

	//是否重复执行
	repeatExec bool

	//任务延迟时间
	delayTime int64

	//下一级时间轮的延迟时间，为0时表示任务到期执行
	nextLevelDelayTime int64

	prev, next *task
}

// taskHead 任务链表头节点
type taskHead struct {
	taskLink *task

	//任务链表存在多个协程并发操作，所以链表操作需要加锁
	lock *sync.Mutex
}

// addNode 添加任务节点
func (t *taskHead) addNode(node *task) {
	t.lock.Lock()
	defer t.lock.Unlock()

	node.next = t.taskLink
	t.taskLink = node
}

// removeNode 移除任务节点
func (t *taskHead) removeNode(removeNode *task) {
	t.lock.Lock()
	defer t.lock.Unlock()

	//移除的是第一个节点、中间、最后节点
	if removeNode.prev == nil {
		t.taskLink = removeNode.next
	} else if removeNode.next == nil {
		removeNode.prev.next = nil
	} else {
		removeNode.prev.next = removeNode.next
	}
}

// taskScaleWheel 任务时间轮关系
//任务所在的时间轮及槽位
type taskScaleWheel struct {
	scale int
	wheel timeWheelType
}

// TimeWheel 时间轮
//这是一个多层级时间轮，最大精度为 秒。由秒、分、时、天级时间轮组成
type TimeWheel struct {
	//多层级时间轮
	wheel *MultilayerTimeWheel

	taskIdScale *sync.Map
	wait        *sync.WaitGroup

	//任务停止通道
	stopCh chan struct{}

	//任务通道
	//接收各层级时间轮任务，带缓冲通道
	taskCh chan *taskHead

	log Log
}

// MultilayerTimeWheel 多层次时间轮
type MultilayerTimeWheel struct {
	size  int //时间轮大小
	tick  int //时间轮指针位置
	scale []*taskHead

	next *MultilayerTimeWheel
}

func NewTimeWheel() *TimeWheel {
	secondWheel := newTimeWheel(secondTimeWheelSize)
	minuteWheel := newTimeWheel(minuteTimeWheelSize)
	hourWheel := newTimeWheel(hourTimeWheelSize)
	dayWheel := newTimeWheel(dayTimeWheelSize)

	secondWheel.next = minuteWheel
	minuteWheel.next = hourWheel
	hourWheel.next = dayWheel

	tw := &TimeWheel{
		wheel:       secondWheel,
		taskIdScale: new(sync.Map),
		stopCh:      make(chan struct{}),
		taskCh:      make(chan *taskHead, 60),
		wait:        &sync.WaitGroup{},
		log:         &logger{logLevel: Debug},
	}

	return tw
}

func newTimeWheel(size int) *MultilayerTimeWheel {
	tw := &MultilayerTimeWheel{
		size:  size,
		tick:  0,
		scale: make([]*taskHead, size),
	}

	for i := range tw.scale {
		tw.scale[i] = &taskHead{
			taskLink: nil,
			lock:     &sync.Mutex{},
		}
	}

	return tw
}

func (tw *TimeWheel) Run() {
	//转动指针
	tw.async(func() {
		timeTicker := time.NewTicker(time.Second)
		defer timeTicker.Stop()

		for {
			select {
			case <-timeTicker.C:
				tw.turnPtr()
			case <-tw.stopCh:
				return
			}
		}
	})

	//处理任务
	tw.async(func() {
		for taskHeadNode := range tw.taskCh {
			tmp := taskHeadNode
			tw.async(func() {
				tw.actTask(tmp)
			})
		}
	})

}

func (tw *TimeWheel) turnPtr() {

	//转动时间轮
	tw.wheel.tick = (tw.wheel.tick + 1) % tw.wheel.size

	//秒
	tw.taskCh <- tw.wheel.scale[tw.wheel.tick]

	tw.log.Info("second:%d\n", tw.wheel.tick)

	if tw.wheel.tick == 0 { //秒级时间轮转完一圈回到0
		//分级时间轮前进一格
		tw.wheel.next.tick = (tw.wheel.next.tick + 1) % tw.wheel.next.size

		//分钟
		tw.taskCh <- tw.wheel.next.scale[tw.wheel.next.tick]

		tw.log.Info("minute:%d\n", tw.wheel.tick)
	}

	if tw.wheel.tick == 0 && tw.wheel.next.tick == 0 { //秒级分级时间轮都转完一圈
		//时级时间轮前进一格
		tw.wheel.next.next.tick = (tw.wheel.next.next.tick + 1) % tw.wheel.next.next.size

		//小时
		tw.taskCh <- tw.wheel.next.next.scale[tw.wheel.next.next.tick]

		tw.log.Info("hour:%d\n", tw.wheel.tick)
	}

	if tw.wheel.tick == 0 && tw.wheel.next.tick == 0 && tw.wheel.next.next.tick == 0 { //秒分时都转完一圈
		//天级时间轮前进一格
		tw.wheel.next.next.next.tick = (tw.wheel.next.next.next.tick + 1) % tw.wheel.next.next.next.size

		//天
		tw.taskCh <- tw.wheel.next.next.next.scale[tw.wheel.next.next.next.tick]

		tw.log.Info("day:%d\n", tw.wheel.tick)
	}
}

func (tw *TimeWheel) actTask(taskHeadNode *taskHead) {

	doTask := func(taskNode *task) {
		//在槽中移除任务
		taskHeadNode.removeNode(taskNode)

		if taskNode.nextLevelDelayTime == 0 {
			tw.async(func() {
				taskNode.taskFunc()

				//判断是否是重复任务
				//重复任务，根据延迟时间重新计算时间轮及槽位
				if taskNode.repeatExec {
					//重新添加任务
					_, _ = tw.add(taskNode.taskFunc, taskNode.repeatExec, taskNode.delayTime, taskNode.taskId)
				}
			})

			return
		}

		//计算下一级槽位,任务降级
		scale, nextLevelDelayTime, wheelType := tw.calculateTimeWheel(taskNode.nextLevelDelayTime)
		taskNode.nextLevelDelayTime = nextLevelDelayTime
		tw.updateTaskScale(taskNode, scale, wheelType)
		tw.updateTaskScaleMap(taskNode.taskId, scale, wheelType)
	}

	tw.rangeTaskNode(taskHeadNode, doTask)
}

// rangeTaskNode 处理任务节点
func (tw *TimeWheel) rangeTaskNode(taskHeadNode *taskHead, f func(node *task)) {
	for taskNode := taskHeadNode.taskLink; taskNode != nil; taskNode = taskNode.next {
		f(taskNode)
	}
}

// updateTaskScale 更新任务时间轮和槽位
func (tw *TimeWheel) updateTaskScale(taskNode *task, scale int, wheelType timeWheelType) {
	switch wheelType {
	case secondType:
		tw.wheel.scale[scale].addNode(taskNode)
	case minuteType:
		tw.wheel.next.scale[scale].addNode(taskNode)
	case hourType:
		tw.wheel.next.next.scale[scale].addNode(taskNode)
	case dayType:
		tw.wheel.next.next.next.scale[scale].addNode(taskNode)
	}
}

func (tw *TimeWheel) Add(f TaskFun, isRepeat bool, delayTime int64) (int64, error) {
	return tw.add(f, isRepeat, delayTime, 0)
}

func (tw *TimeWheel) add(f TaskFun, isRepeat bool, delayTime, taskId int64) (int64, error) {
	if delayTime > dayTimeRange {
		return 0, delayTimeOutOfRange
	}

	if taskId == 0 {
		snowflake := sonyflake.NewSonyflake(sonyflake.Settings{})
		id, err := snowflake.NextID()
		if err != nil {
			tw.log.Error("add:snowflake.NextID:%s\n", err.Error())
			return 0, err
		}

		taskId = int64(id)
	}

	taskNode := &task{
		taskId:             taskId,
		taskFunc:           f,
		repeatExec:         isRepeat,
		delayTime:          delayTime,
		nextLevelDelayTime: 0,
	}

	scale, nextLevelDelayTime, wheelType := tw.calculateTimeWheel(delayTime)

	taskNode.nextLevelDelayTime = nextLevelDelayTime

	tw.updateTaskScale(taskNode, scale, wheelType)
	tw.updateTaskScaleMap(taskId, scale, wheelType)

	return taskId, nil
}

func (tw *TimeWheel) Del(taskId int64) error {
	v, ok := tw.taskIdScale.LoadAndDelete(taskId)
	if !ok {
		return taskIdNoExistErr
	}

	scaleWheel := v.(*taskScaleWheel)

	var taskHeadNode *taskHead

	//获取任务所在的时间轮
	switch scaleWheel.wheel {
	case secondType:
		taskHeadNode = tw.wheel.scale[scaleWheel.scale]
	case minuteType:
		taskHeadNode = tw.wheel.next.scale[scaleWheel.scale]
	case hourType:
		taskHeadNode = tw.wheel.next.next.scale[scaleWheel.scale]
	case dayType:
		taskHeadNode = tw.wheel.next.next.next.scale[scaleWheel.scale]
	}

	delTaskFunc := func(taskNode *task) {
		if taskNode.taskId == taskId {
			taskHeadNode.removeNode(taskNode)
		}
	}

	tw.rangeTaskNode(taskHeadNode, delTaskFunc)

	return nil
}

func (tw *TimeWheel) Stop() {
	tw.stopCh <- struct{}{}
	close(tw.taskCh)

	//等待执行中的任务执行完后再停止
	tw.wait.Wait()
}

func (tw *TimeWheel) updateTaskScaleMap(taskId int64, scale int, wheelType timeWheelType) {
	data := &taskScaleWheel{
		scale: scale,
		wheel: wheelType,
	}
	tw.taskIdScale.Store(taskId, data)
}

// calculateTimeWheel 计算延迟时间的时间轮层级
func (tw *TimeWheel) calculateTimeWheel(delayTime int64) (int, int64, timeWheelType) {
	day, hour, minute, second := tw.secondToDHMS(delayTime)

	var scale int
	var nextLevelDelayTime int64
	var wheelType timeWheelType

	if day != 0 {
		scale = tw.calculateScale(day, tw.wheel.next.next.next.tick, tw.wheel.next.next.next.size)
		nextLevelDelayTime = delayTime - 86400*day
		wheelType = dayType
	} else if hour != 0 {
		scale = tw.calculateScale(hour, tw.wheel.next.next.tick, tw.wheel.next.next.size)
		nextLevelDelayTime = delayTime - 3600*hour
		wheelType = hourType
	} else if minute != 0 {
		scale = tw.calculateScale(minute, tw.wheel.next.tick, tw.wheel.next.size)
		nextLevelDelayTime = delayTime - 60*minute
		wheelType = minuteType
	} else if second != 0 {
		scale = tw.calculateScale(second, tw.wheel.tick, tw.wheel.size)
		nextLevelDelayTime = 0
		wheelType = secondType
	}

	return scale, nextLevelDelayTime, wheelType
}

//scale 槽位计算（时间轮指针位置+需要延迟的时间）% 时间轮长度
func (tw *TimeWheel) calculateScale(delayTime int64, currTick, scaleSize int) int {
	scale := (int64(currTick) + delayTime) % int64(scaleSize)

	return int(scale)
}

func (tw *TimeWheel) async(f func()) {
	tw.wait.Add(1)

	go func() {
		defer func() {
			if err := recover(); err != nil {
				tw.log.Error("%v", err)
			}
		}()

		defer tw.wait.Done()

		f()
	}()
}

// secondToDHMS 秒转换为天 时 分 秒
func (tw *TimeWheel) secondToDHMS(unixTime int64) (day int64, hour int64, minute int64, second int64) {
	if unixTime == 0 {
		return
	}

	oneHour := 60 * 60
	oneMinute := 60
	oneDay := oneHour * 24

	// 天    总秒数/一天的秒数=几天
	day = unixTime / int64(oneDay)
	// 小时  不够一天的秒数可以换算成几小时
	//h := second / int64(oneHour)
	hour = unixTime % int64(oneDay) / int64(oneHour)
	// 分钟  不够一小时的秒数可以换算成几分钟
	minute = unixTime % int64(oneHour) / int64(oneMinute)
	//秒数	不够一分钟的秒数可以换算成几秒
	second = unixTime % int64(oneHour) % int64(oneMinute) % int64(oneMinute)

	return
}
