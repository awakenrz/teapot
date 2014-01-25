package log

import (
    "fmt"
    "teapot/utility"
)

type ChangeMode struct {
    Directory Dir           // the prefix of keys it defines.
    ROQ       string        // Random oracle query of the encryption key.
    Readers   principalList // Key encrypted under each node's public key
    Writers   principalList // Key encrypted under each node's public key
}

const changeModeDebug utility.Debug = true

func (changeMode *ChangeMode) String() string {
    return fmt.Sprintf("ChMod{%v,%v,R{%v},W{%v}}", changeMode.Directory, changeMode.ROQ, changeMode.Readers, changeMode.Writers)
}

func (changeMode *ChangeMode) handle(log *Log, logEntry *LogEntry) error {
    if log.memLog.ReadKeyInfo[logEntry.NodeId] == nil {
        //log.memLog.Modes[logEntry.NodeId] = make(map[Dir][]*LogEntry)
        log.memLog.ReadKeyInfo[logEntry.NodeId] = make(map[Dir][]keyInfo)
        log.memLog.WriteKeyInfo[logEntry.NodeId] = make(map[Dir][]keyInfo)
    }
    writers := make(map[NodeID]bool)
    for nodeId := range changeMode.Writers {
        writers[nodeId] = true
    }
    //log.memLog.Modes[logEntry.NodeId][changeMode.Directory] = append(log.memLog.Modes[logEntry.NodeId][changeMode.Directory], logEntry)
    //changeModeDebug.Debugf("Mode info: %+v", log.memLog.Modes)
    // If I am among the writers
    if key, ok := changeMode.Writers[log.memLog.MyNodeId]; ok {
        // store the key in both writekeyinfo and readkeyinfo
        if myKey, err := extractKey(key, log.memLog.PrivateKey, changeMode.ROQ); err == nil {
            log.memLog.WriteKeyInfo[logEntry.NodeId][changeMode.Directory] = append(log.memLog.WriteKeyInfo[logEntry.NodeId][changeMode.Directory], keyInfo{myKey, logEntry.AcceptStamp, writers})
            log.memLog.ReadKeyInfo[logEntry.NodeId][changeMode.Directory] = append(log.memLog.ReadKeyInfo[logEntry.NodeId][changeMode.Directory], keyInfo{myKey, logEntry.AcceptStamp, writers})
        } else {
            log.memLog.WriteKeyInfo[logEntry.NodeId][changeMode.Directory] = append(log.memLog.WriteKeyInfo[logEntry.NodeId][changeMode.Directory], keyInfo{nil, logEntry.AcceptStamp, writers})
            log.memLog.ReadKeyInfo[logEntry.NodeId][changeMode.Directory] = append(log.memLog.ReadKeyInfo[logEntry.NodeId][changeMode.Directory], keyInfo{nil, logEntry.AcceptStamp, writers})
        }
        // If I am among the readers
    } else {
        // set writekeyinfo to nil
        log.memLog.WriteKeyInfo[logEntry.NodeId][changeMode.Directory] = append(log.memLog.WriteKeyInfo[logEntry.NodeId][changeMode.Directory], keyInfo{nil, logEntry.AcceptStamp, writers})
        if key, ok := changeMode.Readers[log.memLog.MyNodeId]; ok {
            if myKey, err := extractKey(key, log.memLog.PrivateKey, changeMode.ROQ); err == nil {
                log.memLog.ReadKeyInfo[logEntry.NodeId][changeMode.Directory] = append(log.memLog.ReadKeyInfo[logEntry.NodeId][changeMode.Directory], keyInfo{myKey, logEntry.AcceptStamp, writers})
            } else {
                log.memLog.ReadKeyInfo[logEntry.NodeId][changeMode.Directory] = append(log.memLog.ReadKeyInfo[logEntry.NodeId][changeMode.Directory], keyInfo{nil, logEntry.AcceptStamp, writers})
            }
        } else {
            log.memLog.ReadKeyInfo[logEntry.NodeId][changeMode.Directory] = append(log.memLog.ReadKeyInfo[logEntry.NodeId][changeMode.Directory], keyInfo{nil, logEntry.AcceptStamp, writers})
        }
    }
    if len(log.memLog.ReadKeyInfo[logEntry.NodeId][changeMode.Directory]) != len(log.memLog.WriteKeyInfo[logEntry.NodeId][changeMode.Directory]) {
        panic("Should not happen")
    }
    return nil
}

func (changeMode *ChangeMode) asyncHandle(log *Log, logEntry *LogEntry) error {
    return nil
}

func (changeMode *ChangeMode) check(log *Log, logEntry *LogEntry) error {
    return nil
}
