package log

import (
    "errors"
    "fmt"
    "teapot/utility"
)

type Update struct {
    Key         Key
    HashOfValue EncodedHash
    value       []byte
}

const updateDebug utility.Debug = true

func (update *Update) String() string {
    return fmt.Sprintf("Update[%v,%v]", update.Key, update.HashOfValue)
}

func (update *Update) handle(log *Log, logEntry *LogEntry) error {
    log.memLog.Checkpoint[update.Key] = getConcurrentLogEntries(log, logEntry, log.memLog.Checkpoint[update.Key])
    // TO circumvent virtual node id.
    encodedHashOfLogEntry := logEntry.encodedHash()
    virtualNodeId := log.memLog.EntryNodeMap[encodedHashOfLogEntry]
    log.memLog.Checkpoint[update.Key][virtualNodeId] = logEntry
    return nil
}

// exch should check to make sure the value is not synced twice.
func (update *Update) asyncHandle(log *Log, logEntry *LogEntry) error {
    return nil
}

// the writer has the expected priviledge to issue the update.
func (update *Update) check(log *Log, logEntry *LogEntry) error {
    // check the integrity of value.
    if update.value != nil {
        if err := utility.ValidateEncodedHash(update.value, string(update.HashOfValue)); err != nil {
            return updateDebug.Error(err)
        }
    }
    //mode := log.getMode(logEntry)
    if !log.canWrite(logEntry.NodeId, update.Key, logEntry.DVV) {
        if owner, _, _, err := splitKey(update.Key); err != nil {
            return updateDebug.Error(err)
        } else if owner != logEntry.NodeId {
            //logDebug.Debugf("Write Access Denied.")
            return updateDebug.Error(errors.New(fmt.Sprintf("Write Access Denied. You are not supposed to write to this directory (%v).", update.Key)))
        }
        //} else if _, ok := mode.Writers[logEntry.NodeId]; !ok {
        //    return updateDebug.Error(errors.New("Access Denied."))
    }
    return nil
}

// get all that is concurrent with update from updates
func getConcurrentLogEntries(log *Log, logEntry *LogEntry, logEntries entryCollectionByNode) entryCollectionByNode {
    merged := make(entryCollectionByNode)
    if logEntries != nil {
        dvv := make(versionVector)
        dvv = buildFullDvv(log, logEntry.DVV, dvv)
        for nodeId, potentialConcurrentUpdate := range logEntries {
            // I know less about you than you do
            if dvv[nodeId].AcceptStamp < potentialConcurrentUpdate.AcceptStamp {
                merged[nodeId] = potentialConcurrentUpdate
            }
        }
    }
    return merged
}
