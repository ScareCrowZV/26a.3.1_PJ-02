package logger

import (
	"fmt"
	"time"
)

func Log(functionName string, args string) {
	fmt.Printf("%s %s %s \n", time.Now().Format("15:04:05.000"), functionName, args)
}