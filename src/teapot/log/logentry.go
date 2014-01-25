package log

import (
    "crypto/rsa"
    "errors"
    "fmt"
    "teapot/utility"
)

type LogEntry struct {
    /*
       In DVV, virtual node ID are used.
       But one can always fetch real node ID from virtual node id.
    */
    DVV versionVector
    /*
       Can be Update, cDL, Receipt, IVouchForThis, and etc.
    */
    Message IMessage
    /*
       The lamport clock.
    */
    AcceptStamp Timestamp
    NodeId      NodeID
    /*
       The real node ID.
    */
    Sig signature
}

const logEntryDebug utility.Debug = true

// TODO
func (logEntry *LogEntry) String() string {
    return fmt.Sprintf("LE[%v,%v,DVV{%v},%v]", logEntry.AcceptStamp, logEntry.NodeId, logEntry.DVV, logEntry.Message)
}

func (logEntry *LogEntry) serialize() []byte {
    return utility.GobEncode(logEntry)
}

func deserializeLogEntry(buf []byte) (*LogEntry, error) {
    var logEntry LogEntry
    if err := utility.GobDecode(buf, &logEntry); err != nil {
        return nil, err
    }
    return &logEntry, nil
}

func (logEntry *LogEntry) CopyTo(toLogEntry *LogEntry) {
    *toLogEntry = *logEntry
}

func (logEntry *LogEntry) hash() hash {
    buf := []byte(logEntry.String())
    return hash(utility.GetHashOfBytes(buf))
}

func (logEntry *LogEntry) encodedHash() EncodedHash {
    buf := []byte(logEntry.String())
    return EncodedHash(utility.GetHashOfBytesAndEncode(buf))
}

func (logEntry *LogEntry) EncodedHash() EncodedHash {
    return logEntry.encodedHash()
}

func (logEntry *LogEntry) validateHash(expectedHash EncodedHash) error {
    if logEntry.encodedHash() != expectedHash {
        return logEntryDebug.Error(errors.New("Hash mismatch."))
    }
    return nil
}

func (logEntry *LogEntry) sign(privateKey *rsa.PrivateKey) {
    //logEntryDebug.Debugf("Log to sign: %v", logEntry.string())
    if privateKey == nil {
        logEntryDebug.Panicf("Private key is nil")
    }
    buf := []byte(logEntry.String())
    signature, err := utility.SignBinary(buf, privateKey)
    if err != nil {
        logEntryDebug.Panicf("Failed to sign log entry: %+v.\n%v\n", logEntry, err)
    }
    logEntry.Sig = signature
    return
}

func (logEntry *LogEntry) validateSignature(publicKey *rsa.PublicKey) error {
    //logEntryDebug.Debugf("Log to verify: %v", logEntry.string())
    if publicKey == nil {
        return logEntryDebug.Error(errors.New("Public key is nil"))
    }
    signature := logEntry.Sig
    buf := []byte(logEntry.String())
    err := utility.ValidateSignature(buf, publicKey, signature)
    if err != nil {
        return utilityDebug.Error(err)
    }
    return nil
}
