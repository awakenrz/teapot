package log

import (
    "teapot/utility"
)

const joinForkDebug utility.Debug = true

/*
   move log entries of virtualNodeId to a new branch since the ith (inclusive) entry
   A<-B<-C
      |
      |
      B<-C'
   C will be moved to a new branch called newVirtualNodeId.
   B will remain in the branch called oldVirtualNodeId.
   A lot of state in memory needs to be updated accordingly.
   This is only suitable for joining two way forks. Need to be extend to
   join multiple way of forks.
*/
func (log *Log) joinFork(oldVirtualNodeId, newVirtualNodeId NodeID, i int) {
    if i < 0 {
        joinForkDebug.Panicf("Index should not be larger.")
    }
    // join the fork
    log.memLog.SequentialLog[newVirtualNodeId] = log.memLog.SequentialLog[oldVirtualNodeId][i:]
    log.memLog.SequentialLog[oldVirtualNodeId] = log.memLog.SequentialLog[oldVirtualNodeId][:i]

    // Update virtual node id map.
    for _, logEntry := range log.memLog.SequentialLog[newVirtualNodeId] {
        encodedHashOfLogEntry := logEntry.encodedHash()
        log.memLog.EntryNodeMap[encodedHashOfLogEntry] = newVirtualNodeId
        if update, ok := logEntry.Message.(*Update); ok {
            // update checkpoint
            if log.memLog.Checkpoint[update.Key][oldVirtualNodeId] == logEntry {
                delete(log.memLog.Checkpoint[update.Key], oldVirtualNodeId)
                log.memLog.Checkpoint[update.Key][newVirtualNodeId] = logEntry
            }
        }
    }
    _, lastEntry := log.getLastEntryOfNode(newVirtualNodeId)
    if lastEntry != nil {
        hashOfLastEntry := lastEntry.encodedHash()
        // update dvv
        if _, ok := log.memLog.DVV[oldVirtualNodeId]; ok {
            log.memLog.DVV[newVirtualNodeId] = versionInfo{
                lastEntry.AcceptStamp,
                hashOfLastEntry,
            }
            delete(log.memLog.DVV, oldVirtualNodeId)
        }
        // update version vector
        log.memLog.VersionVector[newVirtualNodeId] = versionInfo{
            lastEntry.AcceptStamp,
            hashOfLastEntry,
        }
    } else {
        joinForkDebug.Panicf("Should not happen: if this branch is empty, there is no fork.")
    }
    // update virtual vector for oldVirtualNodeId?
    _, lastEntry = log.getLastEntryOfNode(oldVirtualNodeId)
    if lastEntry != nil {
        hashOfLastEntry := lastEntry.encodedHash()
        log.memLog.VersionVector[oldVirtualNodeId] = versionInfo{
            lastEntry.AcceptStamp,
            hashOfLastEntry,
        }
    } else {
        delete(log.memLog.VersionVector, oldVirtualNodeId)
    }
    return
}

/*
   returns current virtual nodeId
   guarantee: 

   First try to find a logEntry shares common history with current logEntry.
   If such a logEntry doesn't exist, there is no fork.
   If such a logEntry exists, join fork at this point by create two virtual nodes.
   Move the exising logEntry to a new branch.
   Mark the new logEntry to be stored in another branch (in EntryNodeMap.
   The two branches are NodeId.+fork1.EncodedHash() and NodeId.+fork2.EncodedHash()
*/
func (log *Log) detectAndJoinFork(logEntry *LogEntry) (*pOM, NodeID, EncodedHash) {
    // By default, an entry should be stored to the same previous entry
    // Every update is enforced by definition to contain previous update by the same node. otherwise it is a fork.
    encodedHashOfNewLogEntry := logEntry.encodedHash()
    if existingEntry, oldVirtualNodeId, forkIndex := log.getLogEntryShareCommonHistory(logEntry); existingEntry != nil {
        // A fork exists
        encodedHashOfExistingLogEntry := existingEntry.encodedHash()
        joinForkDebug.Debugf("New log entry: %v", logEntry)
        joinForkDebug.Debugf("Existing log entry: %v", existingEntry)
        if encodedHashOfExistingLogEntry != encodedHashOfNewLogEntry {
            pom := newPOM(logEntry.NodeId, encodedHashOfNewLogEntry, encodedHashOfExistingLogEntry)
            log.memLog.BlackList[logEntry.NodeId] = pom
            // This is basically just the concatenation of the two parts.
            newVirtualNodeId1 := getPartialVirtualNodeId(logEntry.NodeId, encodedHashOfExistingLogEntry)
            newVirtualNodeId2 := getPartialVirtualNodeId(logEntry.NodeId, encodedHashOfNewLogEntry)
            joinForkDebug.Debugf("New virtual node id: %v", newVirtualNodeId1)
            joinForkDebug.Debugf("New virtual node id: %v", newVirtualNodeId2)
            if len(log.memLog.SequentialLog[newVirtualNodeId1]) != 0 {
                // the new virtual id must be new, nothing belongs to this virtual id yet.
                joinForkDebug.Panicf("Should not happen: this for should have been observed before.")
            }
            log.joinFork(oldVirtualNodeId, newVirtualNodeId1, forkIndex)
            log.memLog.EntryNodeMap[encodedHashOfNewLogEntry] = newVirtualNodeId2
            return pom, newVirtualNodeId2, encodedHashOfNewLogEntry
        } else {
            // This means this is not even a fork.
            joinForkDebug.Panicf("Should not happen: Not even a fork. This log entry is part of existing history.")
        }
    }
    return nil, logEntry.NodeId, encodedHashOfNewLogEntry
}

/*
   A<-B<-C
      |
      |
   A<-B<-C'
   If I have already seen C, and C' depends on B rather than C, that's a fork.
   For the two dependency thread A<-B<-C and A<-B<-C', A<-B is the common history.
*/
func (log *Log) getLogEntryShareCommonHistory(logEntry *LogEntry) (*LogEntry, NodeID, int) {
    var virtualNodeId NodeID
    var existingEntry *LogEntry = nil
    var forkIndex = -1
    previousLogEntryByTheSameNodeVersionInfo, ok := logEntry.DVV[logEntry.NodeId]
    if !ok {
        // logEntry is the first entry of a node.
        virtualNodeId = logEntry.NodeId
        if len(log.memLog.SequentialLog[virtualNodeId]) > 0 {
            existingEntry = log.memLog.SequentialLog[virtualNodeId][0]
            forkIndex = 0
        }
    } else {
        // logEntry is not the first entry of a node.
        // inspect entry node map to get virtual node id
        virtualNodeId, ok = log.memLog.EntryNodeMap[previousLogEntryByTheSameNodeVersionInfo.HashOfUpdate]
        if !ok {
            // history should already be observed and joined.
            joinForkDebug.Panicf("Should not happen: History should already be observed and joined.")
        }
        // The update after this common history is a fork.
        forkIndex, existingEntry = log.getEntryAfter(virtualNodeId, previousLogEntryByTheSameNodeVersionInfo)
    }
    return existingEntry, virtualNodeId, forkIndex
}
