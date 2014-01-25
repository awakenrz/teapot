package log

import (
    "bytes"
    "crypto/x509"
    "errors"
    "os"
    "strings"
    "teapot/utility"
)

const initDebug utility.Debug = true

/*
   replay log to reconstruct a lot of things
   This includes
   1. in memory log of various form,
   2. local state such as version vector
   3. mode information
   4. Things to reply, things to sync.
*/
func (log *Log) rebuild() error {
    if err := log.recoverSnapshot(); err != nil {
        return initDebug.Error(err)
    }
    if err := log.replayLog(); err != nil {
        return initDebug.Error(err)
    }
    logDebug.Debugf("Log at startup: %+v", log)
    logDebug.Debugf("Checkpoint at startup: %+v", log.memLog.Checkpoint)
    return nil
}

func (log *Log) recoverSnapshot() error {
    // Read the snapshot and deserialize it.
    folder, err := log.sm.GetLastSnapshotFolder()
    if err != nil {
        return initDebug.Error(err)
    }
    if folder != "" {
        if logInMemory, err := log.sm.ReadSnapshot(folder); err != nil {
            return initDebug.Error(err)
        } else {
            // TODO Make sure the memLog is consistent with configuration. Really need to do!!!
            // assign it to log.memLog
            if err := log.checkConfigConsistent(logInMemory); err != nil {
                return initDebug.Error(err)
            }
            log.memLog = logInMemory
        }
    } else {
        log.memLog = newLogInMemory(log.conf)
    }
    initDebug.Debugf("memLog after recovery from snapshot: %+v", log.memLog)
    return nil
}

func (log *Log) checkConfigConsistent(logInMemory *logInMemory) error {
    if logInMemory.MyNodeId != NodeID(log.conf.MyNodeId) {
        return initDebug.Error(errors.New("NodeID has changed."))
    }
    if !bytes.Equal(logInMemory.DefaultKey, log.conf.AESKey) {
        return initDebug.Error(errors.New("Default key has changed."))
    }
    encodedPrivateKey1 := x509.MarshalPKCS1PrivateKey(logInMemory.PrivateKey)
    encodedPrivateKey2 := x509.MarshalPKCS1PrivateKey(log.conf.PrivateKey)
    if !bytes.Equal(encodedPrivateKey1, encodedPrivateKey2) {
        return initDebug.Error(errors.New("Private key has changed."))
    }

    if len(logInMemory.PublicKeys) != len(log.conf.PublicKeys) {
        return initDebug.Error(errors.New("Public keys has changed."))
    }
    for nodeId, publicKey := range logInMemory.PublicKeys {
        encodedPublicKey1, err := x509.MarshalPKIXPublicKey(publicKey)
        if err != nil {
            return initDebug.Error(err)
        }
        encodedPublicKey2, err := x509.MarshalPKIXPublicKey(log.conf.PublicKeys[string(nodeId)])
        if err != nil {
            return initDebug.Error(err)
        }
        if !bytes.Equal(encodedPublicKey1, encodedPublicKey2) {
            return initDebug.Error(errors.New("Public keys has changed."))
        }
    }
    return nil
}

// need to be called after recover from snapshot
func (log *Log) replayLog() error {
    journalIterator := newJournalIterator(log.conf.JournalPath)
    noNeedReply := make(map[EncodedHash]bool)
    noNeedSync := make(map[EncodedHash]bool)
    if journalIterator != nil {
        for {
            line, err := journalIterator.NextJournalEntry()
            if err != nil {
                return initDebug.Error(err)
            }
            if line == "" {
                break
            }
            parts := strings.Split(line, ":")
            if len(parts) != 2 {
                return initDebug.Error(errors.New("Malformed journal."))
            }
            switch parts[0] {
            case "Sync":
                noNeedSync[EncodedHash(parts[1])] = true
            case "Reply":
                noNeedReply[EncodedHash(parts[1])] = true
            default:
                return initDebug.Error(errors.New("Unknown journal type. " + line))
            }
        }
    }
    initDebug.Debugf("noNeedReply: %v", noNeedReply)
    iterator := newLogIterator(log.conf.LogPath)
    if iterator != nil {
        for {
            logEntry, err := iterator.NextLogEntry()
            if err != nil {
                return initDebug.Error(err)
            }
            if logEntry == nil {
                break
            }
            initDebug.Debugf("soft committing: %v", logEntry)
            if err := log.check(logEntry); err != nil {
                return initDebug.Error(err)
            }
            log.updateMemoryState(logEntry)

            encodedHash := logEntry.encodedHash()
            if _, ok := noNeedSync[encodedHash]; !ok {
                log.rs.SyncLogEntry(logEntry)
            }
            if _, ok := noNeedReply[encodedHash]; !ok {
                if err := logEntry.Message.asyncHandle(log, logEntry); err != nil {
                    return initDebug.Error(err)
                }
            }
        }
    }
    valueDir, err := os.Open(log.conf.ValueDir)
    if err != nil {
        return initDebug.Error(err)
    }
    values, err := valueDir.Readdirnames(-1)
    if err != nil {
        return initDebug.Error(err)
    }
    for _, value := range values {
        log.memLog.Values[EncodedHash(value)] = true
    }
    return nil
}

// register types that are going to be persistent
func registerTypes() {
    utility.Register(&Update{})
    utility.Register(&ChangeMode{})
    utility.Register(&pOM{})
    utility.Register(&faultySet{})
    utility.Register(&aCK{})
    utility.Register(&nack{})
    //	utility.Register(&iVouchForThis{})
    utility.Register(&cDL{})
}

func init() {
    registerTypes()
}
