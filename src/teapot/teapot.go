package teapot

/*
   TODO
       // 1. Add joining fork functionality.
       // 2. Add routines to handle S3's down time.
       // 3. Add garbage collection functionality.
       4. Add imformative output.
       5. Add locks because map is not thread safe
       // 6. Persistent timestamp.
*/

import (
    "errors"
    "teapot/conf"
    "teapot/log"
    "teapot/logex"
    "teapot/utility"
    "time"
)

const teapotDebug utility.Debug = true

/* Teapot start here*/
func init() {
}

type ITeapot interface {
    Put(key log.Key, object []byte) error
    Get(key log.Key) ([][]byte, error)
    GetVersion(key log.Key, version int) ([][]byte, error)
    GetVersions() ([]string, error)
    ChangeMode(directory log.Dir, newKey log.SecretKey, readers []log.NodeID, writers []log.NodeID) error
    GC() error
    LS() []log.Key
}

type Teapot struct {
    log   log.ILog
    logEx logex.ILogEx
}

var defaultLogExInterval int = 10
var logExInterval int = defaultLogExInterval
var logExUpperBound int = 64 * defaultLogExInterval

func NewTeapot(config *conf.Config) *Teapot {
    l := log.NewLog(config)
    le := logex.NewLogEx(config, l)

    go func() {
        for {
            if !le.BackgroundGossip() {
                if logExInterval < logExUpperBound {
                    logExInterval *= 2
                }
            } else {
                logExInterval = defaultLogExInterval
            }
            time.Sleep(time.Duration(logExInterval) * time.Second)
        }
    }()
    return &Teapot{l, le}
}

func (teapot *Teapot) GetVersions() ([]string, error) {
    versions, err := teapot.log.GetAllVersions()
    if err != nil {
        return nil, teapotDebug.Error(err)
    }
    return versions, nil
}

func (teapot *Teapot) GC() error {
    return teapot.log.GC()
}

func (teapot *Teapot) LS() []log.Key {
    return teapot.log.LS()
}

func (teapot *Teapot) Put(key log.Key, object []byte) error {
    defer utility.RecordTime("Teapot put latency: %v", time.Now().UnixNano())
    teapotDebug.Debugf("Teapot put size: %v", len(object))
    if err := log.ValidateKey(key); err != nil {
        return teapotDebug.Error(err)
    }
    update := teapot.log.NewUpdate(key, object)
    if update == nil {
        return teapotDebug.Error(errors.New("Failed to generate update."))
    }
    logEntry := teapot.log.NewLogEntry(update)
    if err := teapot.log.Commit(logEntry); err != nil {
        return teapotDebug.Error(err)
    }
    // TODO move this to background of log exchange.
    if err := teapot.log.AsyncHandle(logEntry); err != nil {
        return teapotDebug.Error(err)
    }
    return nil
}

func (teapot *Teapot) Get(key log.Key) ([][]byte, error) {
    defer utility.RecordTime("Teapot get latency: %v", time.Now().UnixNano())
    if err := log.ValidateKey(key); err != nil {
        return nil, teapotDebug.Error(err)
    }
    // Get updates from check point.
    logEntries, err := teapot.log.GetCheckpoint(key)
    if err != nil {
        return nil, teapotDebug.Error(err)
    } else if logEntries != nil {
        return teapot.getValues(logEntries)
    }
    return nil, teapotDebug.Error(errors.New("The key doesn't exist."))
}

func (teapot *Teapot) GetVersion(key log.Key, version int) ([][]byte, error) {
    defer utility.RecordTime("Teapot version latency: %v", time.Now().UnixNano())
    if logEntries, err := teapot.log.GetCheckpointVersion(key, version); err != nil {
        return nil, teapotDebug.Error(err)
    } else if logEntries != nil {
        return teapot.getValues(logEntries)
    }
    return nil, teapotDebug.Error(errors.New("The key doesn't exist."))
}

// build a ChangeMode update and issue the update.
// Assume the key is base64 url encoded
func (teapot *Teapot) ChangeMode(directory log.Dir, newKey log.SecretKey, readers []log.NodeID, writers []log.NodeID) error {
    defer utility.RecordTime("Teapot chmod latency: %v", time.Now().UnixNano())
    changeMode := teapot.log.NewChangeMode(directory, newKey, readers, writers)
    logEntry := teapot.log.NewLogEntry(changeMode)
    if err := teapot.log.Commit(logEntry); err != nil {
        return teapotDebug.Error(err)
    }
    // TODO move this to background of log exchange.
    if err := teapot.log.AsyncHandle(logEntry); err != nil {
        return teapotDebug.Error(err)
    }
    return nil
}

func (teapot *Teapot) getValues(logEntries []*log.LogEntry) ([][]byte, error) {
    defer utility.RecordTime("Teapot getvalues latency: %v", time.Now().UnixNano())
    values := make([][]byte, 0)
    for _, logEntry := range logEntries {
        update, ok := logEntry.Message.(*log.Update)
        if !ok {
            return nil, teapotDebug.Error(errors.New("Non-update in checkpoint"))
        }
        value, err := teapot.log.GetDecryptValue(update, logEntry.DVV)
        if err == nil {
            teapotDebug.Debugf("Teapot get size: %v", len(value))
            values = append(values, value)
            continue
        } else {
            if err.Error() != "Read Access Denied." {
                return nil, teapotDebug.Error(err)
            }
        }
        // TODO read from remote.
        /*value, err = teapot.logEx.GetValue(logEntry.NodeId, update.HashOfValue)
          if err == nil {
              values = append(values, value)
              continue
          } else {
              teapotDebug.Error(err)
          }*/
        teapotDebug.Debugf("The value is probably lost or you don't have access to this value.")
    }
    if len(values) == 0 {
        teapotDebug.Debugf("Not any value read due to value missing from store or access denied.")
        return nil, errors.New("Read Access Denied.")
    }
    return values, nil
}
