package log

import (
    "fmt"
    "teapot/utility"
)

type nack struct {
    Initiator       NodeID
    HashOfFaultySet EncodedHash
    NewPOMs         blackList
}

const nackDebug utility.Debug = true

func (nack *nack) String() string {
    return fmt.Sprintf("NACK[%v,%v,POM{%v}]", nack.Initiator, nack.HashOfFaultySet, nack.NewPOMs)
}

func (nack *nack) handle(log *Log, logEntry *LogEntry) error {
    return nil
}

// TODO reply to nack by issuing a new FaultySet
func (nack *nack) asyncHandle(log *Log, logEntry *LogEntry) error {
    if nack.Initiator == log.memLog.MyNodeId {
        if _, ok := log.memLog.AckFaultySet[nack.HashOfFaultySet]; ok {
            delete(log.memLog.AckFaultySet, nack.HashOfFaultySet)
            // TODO issue new faulty set based on the nackion.
        } else {
            // This means that the nack has already been processed.

        }
    }
    return nil
}

func (nack *nack) check(log *Log, logEntry *LogEntry) error {
    return nil
}
