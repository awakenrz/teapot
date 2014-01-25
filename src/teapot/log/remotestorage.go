package log

import (
    "errors"
    "fmt"
    "runtime"
    "strconv"
    "teapot/adaptor"
    "teapot/conf"
    "teapot/utility"
    "time"
)

const remoteStorageDebug utility.Debug = true

type iRemoteStorage interface {
    SyncLogEntry(logEntry *LogEntry)
    //    GetValue(encodedHash EncodedHash) ([]byte, error)
    StartSyncLog()
}

type remoteStorage struct {
    myNodeId    NodeID
    theAdaptor  adaptor.Adaptor
    logToSync   chan *LogEntry
    valueToSync chan EncodedHash
    js          *journalStorage
    vm          *valueManager
}

/*
   Create a remoteStorage object.
*/
func newRemoteStorage(config *conf.Config, js *journalStorage, vm *valueManager) *remoteStorage {
    auth := adaptor.NewAuth(config.AWSAccessKey, config.AWSSecretKey)
    theAdaptor := adaptor.GetAdaptor(auth, config.MyBucketName)
    return &remoteStorage{
        NodeID(config.MyNodeId),
        theAdaptor,
        make(chan *LogEntry),
        make(chan EncodedHash),
        js,
        vm,
    }
}

/*
   use go routine to optimize this
   asynchronously put the update online.
   fixed synchronization issue with sync value
   We cannot accept an update synced without its value being synced.
   guarantee: if a log is synced, the value associated with it (if any)
   is also synced.
*/
func (rs *remoteStorage) SyncLogEntry(logEntry *LogEntry) {
    encodedHashOfLogEntry := logEntry.encodedHash()
    logDebug.Debugf("Log entry to sync: %v", logEntry)
    logDebug.Debugf("Hash of log entry to sync: %v", encodedHashOfLogEntry)
    go func() {
        rs.logToSync <- logEntry
    }()
}

/*func (rs *remoteStorage) GetValue(encodedHash EncodedHash) ([]byte, error) {
    return rs.theAdaptor.GetBinary(string(encodedHash))
}*/

/*
   Send one entry to remote storage
*/
func (rs *remoteStorage) doSyncLogEntry(encodedHashOfLogEntry EncodedHash, logEntry *LogEntry, logEntryBinary []byte) error {
    if err := rs.theAdaptor.PutBinary(string(encodedHashOfLogEntry), logEntryBinary); err != nil {
        return remoteStorageDebug.Error(err)
    }
    if logEntry.NodeId == rs.myNodeId {
        lastUpdateInfo := strconv.FormatInt(int64(logEntry.AcceptStamp), 10) + "," + string(encodedHashOfLogEntry)
        if err := rs.theAdaptor.PutText(string(rs.myNodeId)+".latestUpdate", lastUpdateInfo); err != nil {
            return remoteStorageDebug.Error(err)
        }
    }
    if err := rs.js.Write("Sync:" + string(encodedHashOfLogEntry)); err != nil {
        return remoteStorageDebug.Error(err)
    }
    return nil
}

/*
   send the value to the remote storage
*/
func (rs *remoteStorage) doSyncValue(encodedHashOfValue EncodedHash) error {
    value, err := rs.vm.ReadValue(encodedHashOfValue)
    if err != nil {
        return remoteStorageDebug.Error(errors.New("The specified value doesn't exist: " + string(encodedHashOfValue)))
    }
    if err := rs.theAdaptor.PutBinary(string(encodedHashOfValue), value); err != nil {
        return remoteStorageDebug.Error(err)
    }
    if err := rs.js.Write("Sync:" + string(encodedHashOfValue)); err != nil {
        return remoteStorageDebug.Error(err)
    }
    return nil
}

// TODO exponetial backup.
func (rs *remoteStorage) StartSyncLog() {
    go func() {
        for {
            logEntry := <-rs.logToSync
            for {
                if logEntry == nil {
                    break
                }
                if update, ok := logEntry.Message.(*Update); ok {
                    // value must be there before update is synced.
                    for {
                        // Sync value
                        remoteStorageDebug.Debugf("Syncing value: %v", update.HashOfValue)
                        if err := rs.doSyncValue(update.HashOfValue); err == nil {
                            break
                        } else {
                            if err.Error() == "The AWS Access Key Id you provided does not exist in our records." {
                                return
                            }
                            remoteStorageDebug.Error(err)
                            runtime.Gosched()
                            time.Sleep(1000000000)
                        }
                    }
                }
                logEntryBinary := utility.GobEncode(logEntry)
                encodedHashOfLogEntry := logEntry.encodedHash()
                remoteStorageDebug.Debugf("Syncing log: %v", encodedHashOfLogEntry)
                if err := rs.doSyncLogEntry(encodedHashOfLogEntry, logEntry, logEntryBinary); err == nil {
                    break
                } else {
                    if err.Error() == "The AWS Access Key Id you provided does not exist in our records." {
                        return
                    }
                    remoteStorageDebug.Error(errors.New(fmt.Sprintf("Syncing log failed: %v\n%v", encodedHashOfLogEntry, err)))
                    runtime.Gosched()
                    time.Sleep(1000000000)
                }
            }
        }
    }()
}
