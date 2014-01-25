package log

import (
    "errors"
    "io/ioutil"
    "os"
    "teapot/utility"
)

const gcDebug utility.Debug = true

func (log *Log) proposeFaultySet() error {
    fs := faultySet{
        log.memLog.BlackList,
    }
    logEntry := log.NewLogEntry(&fs)
    if err := log.Commit(logEntry); err != nil {
        return gcDebug.Error(err)
    }
    if err := log.AsyncHandle(logEntry); err != nil {
        return gcDebug.Error(err)
    }
    return nil
}

func (log *Log) performGC(cdl *cDL) error {
    // Replay logs before the cut in CDL to get a checkpoint of the system.
    // Store everything in the checkpoint into stable storage
    // Backup
    if err := log.takeSnapshot(cdl); err != nil {
        return logDebug.Error(err)
    }

    // Do the real Garbage collection.
    if err := log.garbageCollectValue(cdl); err != nil {
        return logDebug.Error(err)
    }
    // needs to replace current log with new log.
    newLog := NewLog(log.conf)
    if newLog == nil {
        return logDebug.Error(errors.New("Failed to create log after GC."))
    }
    *log = *newLog
    delete(log.memLog.LocalCDLs, cdl.encodedHash())
    return nil
}

/*
   propose a cut to define a prefix for garbage collection by looking at version vectors.
*/
func (log *Log) proposeCut() versionVector {
    validCut := make(versionVector)
    for nodeId := range log.memLog.PublicKeys {
        _, logEntry := log.getLastEntryOfNode(nodeId)
        dvv := make(versionVector)
        dvv = buildFullDvv(log, logEntry.DVV, dvv)
        for dependNodeId, versionInfo := range dvv {
            if _, ok := validCut[dependNodeId]; !ok {
                validCut[dependNodeId] = versionInfo
            } else if versionInfo.AcceptStamp < validCut[dependNodeId].AcceptStamp {
                validCut[dependNodeId] = versionInfo
            }
        }
    }
    return validCut
}

func (log *Log) takeSnapshot(cdl *cDL) error {
    gcDebug.Debugf("Waiting for lock.")
    log.commitLock.Lock()
    defer log.commitLock.Unlock()
    gcDebug.Debugf("Lock acquired.")
    tempLog := newLog(log.conf)
    tempDir, err := ioutil.TempDir("/tmp/", "temp_log")
    if err != nil {
        return gcDebug.Error(err)
    }
    tempLs := newLogStorage(tempDir + "/log.txt")
    if tempLs == nil {
        return gcDebug.Error(errors.New("Fail to create log storage"))
    }
    // First recover from latest snapshot
    if err := tempLog.recoverSnapshot(); err != nil {
        return gcDebug.Error(err)
    }
    // Iterate over log to replay until the CDL
    iterator := newLogIterator(log.conf.LogPath)
    if iterator != nil {
        for {
            logEntry, err := iterator.NextLogEntry()
            if err != nil {
                return gcDebug.Error(err)
            }
            if logEntry == nil {
                break
            }
            encodedHashOfLogEntry := logEntry.encodedHash()
            if cdl.ToBeDeleted[log.memLog.EntryNodeMap[encodedHashOfLogEntry]].AcceptStamp >= logEntry.AcceptStamp {
                if err := tempLog.check(logEntry); err != nil {
                    return gcDebug.Error(err)
                }
                tempLog.updateMemoryState(logEntry)
            } else {
                // write to a new log file.
                if err := tempLs.Append(logEntry); err != nil {
                    return gcDebug.Error(err)
                }
            }
        }
        //gcDebug.Debugf("memLog after replay: %+v", tempLog.memLog)
    } else {
        return gcDebug.Error(errors.New("Cannot read and replay log."))
    }
    // perform "GC" on memLog
    log.memLog.LogIndexedByHash = make(logIndexedByHash)
    for nodeId, logEntries := range tempLog.memLog.SequentialLog {
        lastIndex := len(logEntries) - 1
        if lastIndex >= 0 {
            logEntry := logEntries[lastIndex]
            encodedHash := logEntry.encodedHash()
            tempLog.memLog.SequentialLog[nodeId] = []*LogEntry{logEntry}
            log.memLog.LogIndexedByHash[encodedHash] = logEntry
        }
    }
    newEntryNodeMap := make(entryNodeMap)
    for encodedHash, virtualNodeId := range log.memLog.EntryNodeMap {
        if _, ok := log.memLog.LogIndexedByHash[encodedHash]; !ok {
            newEntryNodeMap[encodedHash] = virtualNodeId
        }
    }
    // TODO more GC on memLog
    gcDebug.Debugf("memLog after clean: %+v", tempLog.memLog)

    // persist the snapshot
    if snapshotPath, err := log.sm.NewSnapshot(tempLog.memLog); err != nil {
        return gcDebug.Error(err)
    } else {
        gcDebug.Debugf("Snapshot persisted at: %v", snapshotPath)
        lastSnapshot, err := log.sm.GetLastSnapshotFolder()
        gcDebug.Debugf("%v, %v", lastSnapshot, err)
    }
    // rename the new log file to log.
    if err := os.Rename(tempLs.logFilePath, log.conf.LogPath); err != nil {
        return gcDebug.Error(err)
    }
    return nil
}

// TODO remove unnecessary values.
func (log *Log) garbageCollectValue(cdl *cDL) error {
    return nil
}
