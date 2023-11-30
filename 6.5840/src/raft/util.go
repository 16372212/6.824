package raft

import (
	"log"
	"time"
)

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		a = append(a, time.Now().UnixMilli())
		log.Printf(format+"  time:【%d】", a...)
	}
	return
}
