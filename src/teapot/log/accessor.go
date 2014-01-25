package log

import (
    "teapot/utility"
)

const accessorDebug utility.Debug = true

// The semantics guarantee that the entry returned has excatly the accept stamp requested.
func (log *Log) getEntryByAcceptstamp(nodeId NodeID, acceptStamp Timestamp) (int, *LogEntry) {
    i := 0
    j := len(log.memLog.SequentialLog[nodeId]) - 1
    for i <= j {
        mid := (i + j) / 2
        if acceptStamp == log.memLog.SequentialLog[nodeId][mid].AcceptStamp {
            return mid, log.memLog.SequentialLog[nodeId][mid]
        }
        if acceptStamp < log.memLog.SequentialLog[nodeId][mid].AcceptStamp {
            j = mid - 1
        } else {
            i = mid + 1
        }
    }
    return -1, nil
}

func (log *Log) getLastEntryOfNode(nodeId NodeID) (int, *LogEntry) {
    index := len(log.memLog.SequentialLog[nodeId]) - 1
    return index, log.getIthEntryOfNode(nodeId, index)
}

func (log *Log) getIthEntryOfNode(nodeId NodeID, i int) *LogEntry {
    if i >= 0 && i < len(log.memLog.SequentialLog[nodeId]) {
        return log.memLog.SequentialLog[nodeId][i]
    }
    return nil
}

// TODO can be optimized with binary search
// Find the log entry after the given version info.
func (log *Log) getEntryAfter(virtualNodeId NodeID, versionInfo versionInfo) (int, *LogEntry) {
    for i := len(log.memLog.SequentialLog[virtualNodeId]) - 1; i >= 0; i-- {
        currentLogEntry := log.memLog.SequentialLog[virtualNodeId][i]
        hashOfPreviousLogEntry := currentLogEntry.DVV[currentLogEntry.NodeId].HashOfUpdate
        // when current log entry is the first log entry, it is definitely true
        if hashOfPreviousLogEntry == versionInfo.HashOfUpdate {
            return i, currentLogEntry
        }
    }
    return -1, nil
}
