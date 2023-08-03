package main

import (
	"fmt"
	"time"
	"time_wheel"
)

func main() {
	t := time_wheel.NewTimeWheel()

	t.Run()

	t.Add(func() {
		fmt.Println("this is 10s")
	}, false, 10)

	tid, _ := t.Add(func() {
		fmt.Println("this is 30s")
	}, true, 30)

	t.Add(func() {
		fmt.Println("this is 1m 20s")
	}, false, 80)

	t.Add(func() {
		fmt.Println("this is 2m")
	}, false, 120)

	//t.Add(func() {
	//	fmt.Println("this is 30m 15s")
	//}, false, 1815)
	//
	//t.Add(func() {
	//	fmt.Println("this is 45m")
	//}, false, 2700)

	//t.Add(func() {
	//	fmt.Println("this is 45m")
	//}, false, 2700)
	//
	//t.Add(func() {
	//	fmt.Println("this is 3h 15m 8s")
	//}, false, 11708)
	//
	//t.Add(func() {
	//	fmt.Println("this is 1d 1h 20m 5s")
	//}, false, 11708)
	//
	time.Sleep(time.Minute)
	t.Del(tid)

	select {}
}
