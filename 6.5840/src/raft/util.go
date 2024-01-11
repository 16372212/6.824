package raft

import (
	"log"
	"time"
)

// Debugging
var Debug = false
var BDebug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		a = append(a, time.Now().UnixMilli())
		log.Printf(format+"  time:【%d】", a...)
	}
	return
}

func BPrintf(format string, a ...interface{}) (n int, err error) {
	if BDebug {
		a = append(a, time.Now().UnixMilli())
		log.Printf(format+"  time:【%d】", a...)
	}
	return
}
