package log

import (
    "fmt"
    "sort"
    "strings"
    "teapot/utility"
)

const faultySetDebug utility.Debug = true

type faultySet struct {
    POMs blackList
}

func (faultySet *faultySet) String() string {
    results := make([]string, len(faultySet.POMs))
    i := 0
    for nodeId, pom := range faultySet.POMs {
        results[i] = fmt.Sprintf("[%v:%v]", nodeId, pom)
        i++
    }
    sort.Strings(results)
    return fmt.Sprintf("FS{POM{%v}}", strings.Join(results, ","))
}

func (faultySet *faultySet) encodedHash() EncodedHash {
    buf := []byte(fmt.Sprint(faultySet.POMs))
    return EncodedHash(utility.GetHashOfBytesAndEncode(buf))
}

func (faultySet *faultySet) handle(log *Log, logEntry *LogEntry) error {
    for nodeId, pom := range faultySet.POMs {
        if _, ok := log.memLog.BlackList[nodeId]; !ok {
            log.memLog.BlackList[nodeId] = pom
        }
    }
    if logEntry.NodeId == log.memLog.MyNodeId {
        // This means that I am the initiator of the gc.
        log.memLog.AckFaultySet[faultySet.encodedHash()] = make(map[NodeID]bool)
    }
    return nil
}

func (faultySet *faultySet) asyncHandle(log *Log, logEntry *LogEntry) error {
    faultySetDebug.Debugf("Handle faultySet.")
    for nodeId := range log.memLog.BlackList {
        if _, ok := faultySet.POMs[nodeId]; !ok {
            // reply missing poms
            faultySetDebug.Debugf("Sending NACK")
            encodedHashOfFaultySet := faultySet.encodedHash()
            nack := nack{
                logEntry.NodeId,
                encodedHashOfFaultySet,
                log.memLog.BlackList,
            }
            replyLogEntry := log.NewLogEntry(&nack)
            // TODO Need to be atomic
            if err := log.Commit(replyLogEntry); err != nil {
                return faultySetDebug.Error(err)
            }
            // It is possible for this update to be committed but not marked as replied.
            if err := log.markAsReplied(logEntry); err != nil {
                return faultySetDebug.Error(err)
            }
            if err := log.AsyncHandle(replyLogEntry); err != nil {
                return faultySetDebug.Error(err)
            }
            // nack replied.
            return nil
        }
    }
    faultySetDebug.Debugf("Sending ACK")
    // reply ack.
    encodedHashOfFaultySet := faultySet.encodedHash()
    ack := aCK{
        logEntry.NodeId,
        encodedHashOfFaultySet,
    }
    replyLogEntry := log.NewLogEntry(&ack)
    // TODO Need to be atomic
    if err := log.Commit(replyLogEntry); err != nil {
        return faultySetDebug.Error(err)
    }
    if err := log.markAsReplied(logEntry); err != nil {
        return faultySetDebug.Error(err)
    }
    if err := log.AsyncHandle(replyLogEntry); err != nil {
        return faultySetDebug.Error(err)
    }
    faultySetDebug.Debugf("Sent ACK")
    return nil
}

func (fs *faultySet) check(log *Log, logEntry *LogEntry) error {
    return nil
}
