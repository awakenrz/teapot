package log

import (
    "crypto/rsa"
    "errors"
    "fmt"
    "sort"
    "strings"
    "teapot/utility"
)

const cdlDebug utility.Debug = true

type cDL struct {
    ToBeDeleted versionVector
    Signatures  map[NodeID]signature
}

func (cdl *cDL) String() string {
    results := make([]string, len(cdl.Signatures))
    i := 0
    for nodeId, sig := range cdl.Signatures {
        results[i] = fmt.Sprintf("[%v:%v]", nodeId, sig)
        i++
    }
    sort.Strings(results)
    return fmt.Sprintf("CDL{TBD{%v},Sig{%v}}", cdl.ToBeDeleted, strings.Join(results, ","))
}

func (cdl *cDL) sign(privateKey *rsa.PrivateKey, nodeId NodeID) {
    buf := []byte(cdl.ToBeDeleted.String())
    signature, err := utility.SignBinary(buf, privateKey)
    if err != nil {
        cdlDebug.Panicf("Failed to sign CDL: %+v.\n%v\n", cdl, err)
    }
    cdl.Signatures[nodeId] = signature
    return
}

func (cdl *cDL) validate(publicKey *rsa.PublicKey, signature []byte) error {
    buf := []byte(cdl.ToBeDeleted.String())
    return utility.ValidateSignature(buf, publicKey, signature)
}

func (cdl *cDL) encodedHash() EncodedHash {
    buf := []byte(cdl.ToBeDeleted.String())
    return EncodedHash(utility.GetHashOfBytesAndEncode(buf))
}

func (cdl *cDL) handle(log *Log, logEntry *LogEntry) error {
    encodedHashOfCDL := cdl.encodedHash()
    localCDL, ok := log.memLog.LocalCDLs[encodedHashOfCDL]
    if !ok {
        cdlDebug.Debugf("The first time see this cdl: %X, %v", encodedHashOfCDL, cdl)
        localCDL = &cDL{cdl.ToBeDeleted, make(map[NodeID]signature)}
        log.memLog.LocalCDLs[encodedHashOfCDL] = localCDL
    }
    // write test case on this.
    localCDL.Signatures[logEntry.NodeId] = cdl.Signatures[logEntry.NodeId]
    return nil
}

// check if there is any new signatures, if get any new updates, check if fully signed.
// start gc or not
// If id didn't sign it, sign it and reply.
func (cdl *cDL) asyncHandle(log *Log, logEntry *LogEntry) error {
    encodedHashOfCDL := cdl.encodedHash()
    localCDL, _ := log.memLog.LocalCDLs[encodedHashOfCDL]
    // I haven't signed yet. Need to make sure I can sign.
    if _, ok := localCDL.Signatures[log.memLog.MyNodeId]; !ok {
        // Validate every updates in CDL can be discarded.
        for _, versionInfo := range localCDL.ToBeDeleted {
            // if logEntry is not ready to be deleted return without signing the cdl
            logEntryToDelete := log.GetEntryByEncodedHash(versionInfo.HashOfUpdate)
            if !readyToDelete(logEntryToDelete) {
                // TODO reject the cdl.
                // This cannot happen for a correct node
                return nil
            }
        }
        // Once signed, promise not to accept updates that contradicts the proposed cut.
        // TODO promised not to accept contradicts updates.
        newCDL := cDL{
            cdl.ToBeDeleted,
            make(map[NodeID]signature),
        }

        newCDL.sign(log.memLog.PrivateKey, log.memLog.MyNodeId)
        cdlDebug.Debugf("Sign done. committing.")
        // issue new CDL
        replyLogEntry := log.NewLogEntry(&newCDL)
        if err := log.Commit(replyLogEntry); err != nil {
            return cdlDebug.Error(err)
        }
        if err := log.markAsReplied(logEntry); err != nil {
            return cdlDebug.Error(err)
        }
        if err := log.AsyncHandle(replyLogEntry); err != nil {
            return cdlDebug.Error(err)
        }
    }
    // See if fully signed.
    if fullySigned(localCDL, log, log.memLog.PublicKeys) {
        // start gc.
        if err := log.markAsReplied(logEntry); err != nil {
            return cdlDebug.Error(err)
        }
        return log.performGC(cdl)
    }
    return nil
}

func fullySigned(cdl *cDL, log *Log, allNode map[NodeID]*rsa.PublicKey) bool {
    // Nodes not in blacklist should sign.
    for nodeId := range allNode {
        if _, ok := log.memLog.BlackList[nodeId]; !ok {
            if signature, ok := cdl.Signatures[nodeId]; ok {
                err := cdl.validate(log.memLog.PublicKeys[nodeId], signature)
                if err != nil {
                    cdlDebug.Panicf("Should not happen, every signature here should have been validated.")
                }
            } else {
                cdlDebug.Debugf("Not fully signed. Missing signature from %v", nodeId)
                return false
            }
        }
    }
    cdlDebug.Debugf("Fully signed.")
    return true
}

// TODO ready to delete?
func readyToDelete(logEntry *LogEntry) bool {
    return true
}

func (cdl *cDL) check(log *Log, logEntry *LogEntry) error {
    if signature, ok := cdl.Signatures[logEntry.NodeId]; ok {
        err := cdl.validate(log.memLog.PublicKeys[logEntry.NodeId], signature)
        if err != nil {
            // TODO block the node
            return cdlDebug.Error(err)
        }
    } else {
        // TODO block the node
        return cdlDebug.Error(errors.New("CDL not containing signature from the node."))
    }
    return nil
}
