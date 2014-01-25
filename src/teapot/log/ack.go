package log

import (
    "fmt"
    "teapot/utility"
)

const ackDebug utility.Debug = true

type aCK struct {
    Initiator       NodeID
    HashOfFaultySet EncodedHash
}

func (ack *aCK) String() string {
    return fmt.Sprintf("ACK[%v,%v]", ack.Initiator, ack.HashOfFaultySet)
}

// TODO
func (ack *aCK) handle(log *Log, logEntry *LogEntry) error {
    if ack.Initiator == log.memLog.MyNodeId {
        if _, ok := log.memLog.AckFaultySet[ack.HashOfFaultySet]; ok {
            log.memLog.AckFaultySet[ack.HashOfFaultySet][logEntry.NodeId] = true
        } else {
            // At this point, it means that this session of garbage collection has completed.
            //log.memLog.AckFaultySet[ack.HashOfFaultySet] = make(map[NodeID]bool)
        }
    }
    return nil
}

// reply to ack
func (ack *aCK) asyncHandle(log *Log, logEntry *LogEntry) error {
    if ack.Initiator == log.memLog.MyNodeId {
        // If the faulty set is deleted from this data structure, it means that it has been fully accpeted or rejected.
        if _, ok := log.memLog.AckFaultySet[ack.HashOfFaultySet]; ok {
            // if fully accepted, proceed to propose cdl.
            if len(log.memLog.AckFaultySet[ack.HashOfFaultySet]) >= len(log.memLog.PublicKeys)-len(log.memLog.BlackList) {
                // all nodes that are not blacklisted have acked.
                for nodeId := range log.memLog.PublicKeys {
                    if _, ok := log.memLog.BlackList[nodeId]; !ok {
                        if _, ok := log.memLog.AckFaultySet[ack.HashOfFaultySet][nodeId]; !ok {
                            return nil
                        }
                    }
                }
                delete(log.memLog.AckFaultySet, ack.HashOfFaultySet)
                // fully accpeted. Call proposeCut to propose a cdl
                toBeDeleted := log.proposeCut()
                cdl := cDL{
                    toBeDeleted,
                    make(map[NodeID]signature),
                }
                encodedHashOfCDL := cdl.encodedHash()
                cdl.sign(log.memLog.PrivateKey, log.memLog.MyNodeId)
                log.memLog.LocalCDLs[encodedHashOfCDL] = &cdl

                // TODO need to be atomic
                replyLogEntry := log.NewLogEntry(&cdl)
                if err := log.Commit(replyLogEntry); err != nil {
                    return err
                }
                if err := log.markAsReplied(logEntry); err != nil {
                    return err
                }
                if err := log.AsyncHandle(replyLogEntry); err != nil {
                    return err
                }
            }
        }
    }
    return nil
}

func (ack *aCK) check(log *Log, logEntry *LogEntry) error {
    return nil
}
