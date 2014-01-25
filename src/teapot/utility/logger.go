package utility

import (
    "fmt"
    "log"
    "runtime"
    "time"
)

type Debug bool

const loggerDebug Debug = true

func RecordTime(formatString string, start int64) {
    loggerDebug.Debugf(formatString, time.Now().UnixNano()-start)
}

func (d Debug) Debugf(format string, args ...interface{}) {
    if d {
        formatLog(format, args...)
    }
}

func (d Debug) Error(err error) error {
    if d {
        formatLog("%v", err)
    }
    return err
}

func (d Debug) Panicf(format string, args ...interface{}) {
    log.Panicf(format, args...)
}

func formatLog(format string, args ...interface{}) {
    if _, file, line, ok := runtime.Caller(2); ok {
        location := fmt.Sprintf("%v line %v: ", file, line)
        log.Printf(location+format, args...)
    } else {
        log.Printf(format, args...)
    }
}
