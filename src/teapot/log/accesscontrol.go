package log

import (
    "sort"
    "teapot/utility"
)

const acDebug utility.Debug = true

func (log *Log) canWrite(nodeId NodeID, key Key, dvv versionVector) bool {
    owner, directory, _, err := splitKey(key)
    if err != nil {
        acDebug.Debugf("%v", err.Error())
        return false
    }
    if owner == nodeId {
        return true
    }
    acDebug.Debugf("Key in interest: %v", key)
    acDebug.Debugf("Write Key Info: %v", log.memLog.WriteKeyInfo[owner][directory])
    fullDVV := make(versionVector)
    fullDVV = buildFullDvv(log, dvv, fullDVV)
    acDebug.Debugf("Full DVV: %v", fullDVV)
    versionOfOwner := fullDVV[owner]
    length := len(log.memLog.WriteKeyInfo[owner][directory])
    acDebug.Debugf("NodeId: %v, Key: %v, AcceptStamp: %v, Length: %v", nodeId, key, versionOfOwner.AcceptStamp, length)
    if length > 0 {
        // Find the last key info whose accept stamp is smaller or equal to
        // than version.acceptStamp
        index := sort.Search(length, func(i int) bool {
            return log.memLog.WriteKeyInfo[owner][directory][i].AcceptStamp > versionOfOwner.AcceptStamp
        })
        if index > 0 {
            if _, ok := log.memLog.WriteKeyInfo[owner][directory][index-1].Writers[nodeId]; ok {
                return true
            }
        }
    }
    return false
}

// This accept stamp is supposed to be the accept stamp of the dependent update from the owner.
func (log *Log) getReadKey(key Key, dvv versionVector) SecretKey {
    owner, directory, _, err := splitKey(key)
    if err != nil {
        acDebug.Debugf("%v", err.Error())
        return nil
    }
    acDebug.Debugf("Key in interest: %v", key)
    acDebug.Debugf("Read Key Info: %v", log.memLog.ReadKeyInfo[owner][directory])
    fullDVV := make(versionVector)
    fullDVV = buildFullDvv(log, dvv, fullDVV)
    acDebug.Debugf("Full DVV: %v", fullDVV)
    versionOfOwner := fullDVV[owner]
    length := len(log.memLog.ReadKeyInfo[owner][directory])
    acDebug.Debugf("Key: %v, AcceptStamp: %v, Length: %v", key, versionOfOwner.AcceptStamp, length)
    if length > 0 {
        // Find the last key info whose accept stamp is smaller
        // than acceptStamp
        index := sort.Search(length, func(i int) bool {
            return log.memLog.ReadKeyInfo[owner][directory][i].AcceptStamp > versionOfOwner.AcceptStamp
        })
        if index > 0 {
            return log.memLog.ReadKeyInfo[owner][directory][index-1].Key
        }
    }
    if owner == log.memLog.MyNodeId {
        return log.memLog.DefaultKey
    }
    return nil
}

/*// Return nil if I cannot read the key
func (log *Log) getCurrentReadKey(key Key) SecretKey {
	owner, directory, _ := splitKey(key)
	acDebug.Debugf("Key Info: %v", log.readKeyInfo)
	acDebug.Debugf("Mode Info: %v", log.modes)
	lastIndex := len(log.readKeyInfo[owner][directory]) - 1
	if lastIndex >= 0 {
		return log.readKeyInfo[owner][directory][lastIndex].Key
	}
	if owner == log.myNodeId {
		return log.defaultKey
	}
	return nil
}*/

/*
// This accept stamp is supposed to be the accept stamp of the dependent update from the owner.
func (log *Log) getWriteKey(key Key, acceptStamp Timestamp) SecretKey {
    owner, directory, _ := splitKey(key)
    acDebug.Debugf("Write Key Info: %v", log.memLog.WriteKeyInfo[owner][directory])
    //	acDebug.Debugf("Mode Info: %v", log.memLog.Modes)
    length := len(log.memLog.WriteKeyInfo[owner][directory])
    if length > 0 {
        // Find the last key info whose accept stamp is smaller
        // than acceptStamp
        index := sort.Search(length, func(i int) bool { return log.memLog.WriteKeyInfo[owner][directory][i].AcceptStamp > acceptStamp })
        if index > 0 {
            return log.memLog.WriteKeyInfo[owner][directory][index-1].Key
        }
    }
    if owner == log.memLog.MyNodeId {
        return log.memLog.DefaultKey
    }
    return nil
}*/

// Return default key if no change mode and is the owner
// Return nil if I cannot write the key
func (log *Log) getCurrentWriteKey(key Key) SecretKey {
    owner, directory, _, err := splitKey(key)
    if err != nil {
        acDebug.Debugf("%v", err.Error())
    }
    acDebug.Debugf("Write Key Info: %v", log.memLog.WriteKeyInfo[owner][directory])
    //	acDebug.Debugf("Mode Info: %v", log.memLog.Modes)
    lastIndex := len(log.memLog.WriteKeyInfo[owner][directory]) - 1
    if lastIndex >= 0 {
        return log.memLog.WriteKeyInfo[owner][directory][lastIndex].Key
    }
    if owner == log.memLog.MyNodeId {
        return log.memLog.DefaultKey
    }
    return nil
}

// Find the owner of object the logEntry tries to update
// Find the update (u) from the owner logEntry depends on.
// Find the latest change mode udpate before u issued by the owner.
// This is the mode of the current logEntry
/*func (log *Log) getMode(logEntry *LogEntry) *ChangeMode {
	update, ok := logEntry.Message.(*Update)
	if !ok {
		acDebug.Debugf("Non update log entries does not have a mode")
		return nil
	}
	owner, directory, _ := splitKey(update.Key)
	dvv := make(versionVector)
    dvv = buildFullDvv(log, logEntry, dvv)
	acceptStamp := dvv[owner].AcceptStamp
    index, _ := log.getEntryByAcceptstamp(owner, acceptStamp)
	// TODO can be optimized? with binary search?
	for i := index; i > 0; i-- {
		if changeMode, ok := log.memLog.SequentialLog[owner][i].Message.(*ChangeMode); ok {
			if changeMode.Directory == directory {
				return changeMode
			}
		}
	}
	return nil
}*/
