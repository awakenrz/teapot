package log

import (
    "bytes"
    "crypto/rand"
    "crypto/rsa"
    "encoding/base64"
    "errors"
    "fmt"
    "io/ioutil"
    "strings"
    "sync"
    "teapot/conf"
    "teapot/utility"
    "time"
)

const logDebug utility.Debug = true

type ILog interface {
    NewLogEntry(message IMessage) *LogEntry
    NewUpdate(key Key, value []byte) *Update
    NewChangeMode(directory Dir, newKey SecretKey, readers []NodeID, writers []NodeID) *ChangeMode
    Commit(logEntry *LogEntry) error
    AsyncHandle(logEntry *LogEntry) error
    Blocked(nodeId NodeID) bool
    GetCheckpoint(key Key) ([]*LogEntry, error)
    GetCheckpointVersion(key Key, version int) ([]*LogEntry, error)
    LS() []Key
    GetAllVersions() ([]string, error)
    GetValue(encodedHashOfValue EncodedHash) ([]byte, error)
    GetDecryptValue(update *Update, dvv versionVector) ([]byte, error)
    WriteValue(encodedHashOfValue EncodedHash, value []byte) error
    GetLastEntryInfoOfNode(nodeId NodeID) *VersionInfo
    GetEntryByEncodedHash(encodedHash EncodedHash) *LogEntry
    Observed(nodeId NodeID, acceptStamp Timestamp) bool
    HasLogEntry(nodeId NodeID, encodedHash EncodedHash) bool
    GC() error
}

type logInMemory struct {
    MyNodeId   NodeID
    PrivateKey *rsa.PrivateKey
    PublicKeys map[NodeID]*rsa.PublicKey

    /* Need persistent, all others can be reconstruct from replaying log. */
    SequentialLog sequentialLog
    /* State in memory, can be rebuild during startup */
    LogIndexedByHash logIndexedByHash
    EntryNodeMap     entryNodeMap
    DVV              versionVector
    VersionVector    versionVector
    /*
       Local lamport clock
    */
    LogicalClock Timestamp
    /*
       Local check point.
    */
    Checkpoint checkpoint
    DefaultKey SecretKey

    // Maps encryption key to the key-value pair key prefix.
    // maps id+directory to a list of keys
    ReadKeyInfo  map[NodeID]map[Dir][]keyInfo
    WriteKeyInfo map[NodeID]map[Dir][]keyInfo
    //	Modes           map[NodeID]map[Dir][]*LogEntry

    Values map[EncodedHash]bool

    // used for Join fork
    BlackList blackList
    // Store partially replied CDLs. All cdls contain signatures.
    LocalCDLs map[EncodedHash]*cDL

    ProposedFaultySet map[EncodedHash]faultySet
    // Store partially replied FaultySets.
    AckFaultySet map[EncodedHash]map[NodeID]bool
}

type Log struct {
    memLog *logInMemory
    /*	myNodeId        NodeID
    	privateKey      *rsa.PrivateKey
    	publicKeys      map[NodeID]*rsa.PublicKey

    	/* Need persistent, all others can be reconstruct from replaying log.
    	sequentialLog sequentialLog
    	/* State in memory, can be rebuild during startup 
    	logIndexedByHash logIndexedByHash
    	entryNodeMap     entryNodeMap
    	dVV              versionVector
    	versionVector    versionVector
    	/*
    	   Local lamport clock
    	logicalClock Timestamp
    	/*
    	   Local check point.
    	checkpoint checkpoint
    	defaultKey SecretKey

    	// Maps encryption key to the key-value pair key prefix.
    	// maps id+directory to a list of keys
    	readKeyInfo     map[NodeID]map[Dir][]keyInfo
    	writeKeyInfo    map[NodeID]map[Dir][]keyInfo
    	modes           map[NodeID]map[Dir][]ChangeMode

    	values  map[EncodedHash]bool

    	blackList   blackList
    	// All cdls contains all signatures.
    	localcDLs           map[EncodedHash]cDL
    	proposedFaultySet   map[EncodedHash]faultySet
    	ackFaultySet        map[EncodedHash]map[NodeID]bool*/

    conf       *conf.Config
    commitLock *sync.Mutex
    gcMutex    *sync.Mutex

    /*
       local and remote storage.
    */
    ls  iLogStorage
    js  iJournalStorage
    vm  iValueManager
    sm  iSnapshotManager
    rs  iRemoteStorage
}

func (log *Log) LS() []Key {
    results := make([]Key, 0)
    for key := range log.memLog.Checkpoint {
        results = append(results, key)
    }
    return results
}

func (log *Log) GetDecryptValue(update *Update, dvv versionVector) ([]byte, error) {
    defer utility.RecordTime("Decryption latency: %v", time.Now().UnixNano())
    if secretKey := log.getReadKey(update.Key, dvv); secretKey != nil {
        value, err := log.GetValue(update.HashOfValue)

        if err != nil {
            return nil, logDebug.Error(err)
        }
        // Decrypt
        buf := bytes.NewReader(value)
        reader, err := utility.NewDecryptReader(buf, secretKey)
        if err != nil {
            return nil, logDebug.Error(err)
        }
        value, err = ioutil.ReadAll(reader)
        if err != nil {
            return nil, logDebug.Error(err)
        }
        return value, nil
    }
    //    logDebug.Debugf("Read Access Denied.")
    return nil, logDebug.Error(errors.New(fmt.Sprintf("Read Access Denied. You are not suppose to read entries in this directory (%v).", update.Key)))
}

func newLogInMemory(config *conf.Config) *logInMemory {
    publicKeys := make(map[NodeID]*rsa.PublicKey)
    for nodeId, publicKey := range config.PublicKeys {
        publicKeys[NodeID(nodeId)] = publicKey
    }
    return &logInMemory{
        NodeID(config.MyNodeId),
        config.PrivateKey,
        publicKeys,
        make(sequentialLog),
        make(logIndexedByHash),
        make(entryNodeMap),
        make(versionVector),
        make(versionVector),
        Timestamp(1),
        make(checkpoint),
        SecretKey(config.AESKey),
        make(map[NodeID]map[Dir][]keyInfo),
        make(map[NodeID]map[Dir][]keyInfo),
        //        make(map[NodeID]map[Dir][]*LogEntry),

        make(map[EncodedHash]bool),

        make(blackList),
        make(map[EncodedHash]*cDL),
        make(map[EncodedHash]faultySet),
        make(map[EncodedHash]map[NodeID]bool),
    }
}

// TODO call startSyncLog
func NewLog(config *conf.Config) *Log {
    log := newLog(config)
    if err := log.rebuild(); err != nil {
        logDebug.Debugf("Failed to rebuild log: %v", err)
        return nil
    }
    log.rs.StartSyncLog()
    return log
}

func newLog(config *conf.Config) *Log {
    ls := newLogStorage(config.LogPath)
    js := newJournalStorage(config.JournalPath)
    vm := newValueManager(config.ValueDir)
    sm := newSnapshotManager(config.SnapshotDir)
    rs := newRemoteStorage(config, js, vm)

    log := Log{
        nil,
        config,
        new(sync.Mutex),
        new(sync.Mutex),
        ls,
        js,
        vm,
        sm,
        rs,
    }
    return &log
}

func (log *Log) NewLogEntry(message IMessage) *LogEntry {
    logEntry := LogEntry{}
    logEntry.NodeId = log.memLog.MyNodeId
    logEntry.Message = message
    logEntry.AcceptStamp = -1
    return &logEntry
}

func (log *Log) NewUpdate(key Key, value []byte) *Update {
    owner, dir, path, err := splitKey(key)
    if err != nil {
        logDebug.Debugf("%v", err.Error())
        return nil
    }
    key = Key(strings.Join([]string{string(owner), string(dir), string(path)}, "/"))
    logDebug.Debugf("Key after cleaned: %v", key)
    //hashOfValue := EncodedHash(utility.GetHashOfBytesAndEncode(value))
    // Add the value to temp store.
    // The value is going to be removed from temp store
    // when the value is persisted.
    //log.tempValueStore[hashOfValue] = value
    update := Update{
        key,
        EncodedHash(""),
        value,
    }
    return &update
}

// Owner will automatically be a writer.
func (log *Log) NewChangeMode(directory Dir, newKey SecretKey, readers []NodeID, writers []NodeID) *ChangeMode {
    readerMap := make(principalList)
    writerMap := make(principalList)
    roq := utility.GetHashOfBytesAndEncode(newKey)
    // Need to access public keys
    for _, writer := range writers {
        key, err := rsa.EncryptPKCS1v15(rand.Reader, log.memLog.PublicKeys[NodeID(writer)], newKey)
        if err != nil {
            logDebug.Error(err)
            return nil
        }
        writerMap[NodeID(writer)] = encodedEncryptedSecretKey(base64.URLEncoding.EncodeToString(key))
    }
    if _, ok := writerMap[log.memLog.MyNodeId]; !ok {
        key, err := rsa.EncryptPKCS1v15(rand.Reader, log.memLog.PublicKeys[log.memLog.MyNodeId], newKey)
        if err != nil {
            logDebug.Error(err)
            return nil
        }
        writerMap[log.memLog.MyNodeId] = encodedEncryptedSecretKey(base64.URLEncoding.EncodeToString(key))
    }
    for _, reader := range readers {
        if _, ok := writerMap[reader]; ok {
            continue
        }
        key, err := rsa.EncryptPKCS1v15(rand.Reader, log.memLog.PublicKeys[NodeID(reader)], newKey)
        if err != nil {
            logDebug.Error(err)
            return nil
        }
        readerMap[NodeID(reader)] = encodedEncryptedSecretKey(base64.URLEncoding.EncodeToString(key))
    }
    changeMode := ChangeMode{
        directory,
        roq,
        readerMap,
        writerMap,
    }
    return &changeMode
}

func (log *Log) encryptValue(update *Update) error {
    defer utility.RecordTime("Encryption latency: %v", time.Now().UnixNano())
    if secretKey := log.getCurrentWriteKey(update.Key); secretKey != nil {
        var buf bytes.Buffer
        writer, err := utility.NewEncryptWriter(&buf, secretKey)
        if err != nil {
            return logDebug.Error(err)
        }
        writer.Write(update.value)
        encryptedValue := buf.Bytes()
        update.HashOfValue = EncodedHash(utility.GetHashOfBytesAndEncode(encryptedValue))
        update.value = encryptedValue
    } else {
        return logDebug.Error(errors.New(fmt.Sprintf("Write Access Denied. You are not supposed to write to this directory (%v).", update.Key)))
    }
    return nil
}

/*
   Commit determines the position of a log entry in the node's history
   and the log entry is visible to others after it is committed.
   guarantee: if the log entry is persisted, should not return any error.
*/
func (log *Log) Commit(logEntry *LogEntry) error {
    defer utility.RecordTime("Log commit latency: %v", time.Now().UnixNano())
    // lock
    log.commitLock.Lock()
    defer log.commitLock.Unlock()
    if logEntry.AcceptStamp == -1 {
        // My own log entry, no need to check
        if logEntry.NodeId != log.memLog.MyNodeId {
            return logDebug.Error(errors.New("Should not happen: log entry from other node should have everything filled up."))
        }
        update, ok := logEntry.Message.(*Update)
        if ok {
            if err := log.encryptValue(update); err != nil {
                return logDebug.Error(err)
            }
            /*// Encrypt the value
              if secretKey := log.getCurrentWriteKey(update.Key); secretKey != nil {
                  var buf bytes.Buffer
                  writer, err := utility.NewEncryptWriter(&buf, secretKey)
                  if err != nil {
                      return logDebug.Error(err)
                  }
                  writer.Write(update.value)
                  encryptedValue := buf.Bytes()
                  update.HashOfValue = EncodedHash(utility.GetHashOfBytesAndEncode(encryptedValue))
                  update.value = encryptedValue
              } else {
                  return logDebug.Error(errors.New(fmt.Sprintf("Write Access Denied. You are not supposed to write to this directory (%v).", update.Key)))
              }*/
        }
        logEntry.AcceptStamp = log.memLog.LogicalClock
        logEntry.DVV = log.memLog.DVV
        logEntry.sign(log.memLog.PrivateKey)
    }
    logDebug.Debugf("Log to commit at %v: %v", log.memLog.MyNodeId, logEntry)
    if err := log.check(logEntry); err != nil {
        return logDebug.Error(err)
    }

    if err := log.persist(logEntry); err != nil {
        return logDebug.Error(err)
    }
    // Only check and update memory state is necessary in recovery
    // What is the guarantee if exists here?
    log.updateMemoryState(logEntry)
    return nil
}

/*
   What is the guarantee?
   guarantee: there should not be any error here.
   TODO this seems not the case. New updates may be issued, and thus error can be returned due to not able to commit some updates, or other errors.

*/
// check the journal to find out things that are already synced and replied.
// need in recovery
func (log *Log) AsyncHandle(logEntry *LogEntry) error {
    if log.needSync(logEntry) {
        log.rs.SyncLogEntry(logEntry)
    }
    if log.needReply(logEntry) {
        return logEntry.Message.asyncHandle(log, logEntry)
    }
    return nil
}

func (log *Log) GetAllVersions() ([]string, error) {
    version, err := log.sm.GetAllSnapshotFolders()
    if err != nil {
        return nil, logDebug.Error(err)
    }
    return version, nil
}

func (log *Log) GetCheckpoint(key Key) ([]*LogEntry, error) {
    if updates, ok := log.memLog.Checkpoint[key]; ok {
        results := make([]*LogEntry, 0)
        for _, update := range updates {
            results = append(results, update)
        }
        return results, nil
    }
    return nil, logDebug.Error(errors.New(fmt.Sprintf("The key doesn't exist. %v", key)))
}

func (log *Log) GetCheckpointVersion(key Key, version int) ([]*LogEntry, error) {
    allSnapshots, err := log.sm.GetAllSnapshotFolders()
    if err != nil {
        return nil, logDebug.Error(err)
    }
    if version >= len(allSnapshots) {
        return nil, logDebug.Error(errors.New("The specified version doesn't exist."))
    }
    memLog, err := log.sm.ReadSnapshot(allSnapshots[version])
    if err != nil {
        return nil, logDebug.Error(err)
    }
    if updates, ok := memLog.Checkpoint[key]; ok {
        results := make([]*LogEntry, 0)
        for _, update := range updates {
            results = append(results, update)
        }
        return results, nil
    }
    return nil, logDebug.Error(errors.New("No such key."))
}

func (log *Log) Blocked(nodeId NodeID) bool {
    if _, ok := log.memLog.BlackList[nodeId]; ok {
        return true
    }
    return false
}

func (log *Log) GetLastEntryInfoOfNode(nodeId NodeID) *VersionInfo {
    _, lastEntry := log.getLastEntryOfNode(nodeId)
    if lastEntry != nil {
        return &VersionInfo{lastEntry.AcceptStamp, lastEntry.encodedHash()}
    }
    return nil
}

// read the value from disk.
// Reading values from remote is not done in this component.
func (log *Log) GetValue(encodedHashOfValue EncodedHash) ([]byte, error) {
    if _, ok := log.memLog.Values[encodedHashOfValue]; ok {
        // read from disk
        value, err := log.vm.ReadValue(encodedHashOfValue)
        if err != nil {
            return nil, err
        }
        return value, nil
    }
    return nil, logDebug.Error(errors.New("This value is not available on this node."))
}

func (log *Log) WriteValue(encodedHashOfValue EncodedHash, value []byte) error {
    if err := log.vm.WriteValue(encodedHashOfValue, value); err != nil {
        return logDebug.Error(err)
    }
    log.memLog.Values[encodedHashOfValue] = true
    return nil
}

func (log *Log) GetEntryByEncodedHash(encodedHash EncodedHash) *LogEntry {
    return log.memLog.LogIndexedByHash[encodedHash]
}

/*
   return true if the log entry has been observed by this node.
*/
func (log *Log) Observed(nodeId NodeID, acceptStamp Timestamp) bool {
    if versionInfo, ok := log.memLog.VersionVector[nodeId]; ok {
        return versionInfo.AcceptStamp >= acceptStamp
    }
    return false
}

func (log *Log) HasLogEntry(nodeId NodeID, encodedHash EncodedHash) bool {
    if existingLogEntry, ok := log.memLog.LogIndexedByHash[encodedHash]; ok {
        // do some deeper check
        if existingLogEntry.NodeId == nodeId {
            return true
        }
    }
    return false
}

/*
   1. Propose faulty set
   2. Collect ack.
   3. Propose cdl
   4. Collect signature
   5. clean everything.
*/
func (log *Log) GC() error {
    if err := log.proposeFaultySet(); err != nil {
        return logDebug.Error(err)
    }
    return nil
}

func (log *Log) serialize() []byte {
    return utility.GobEncode(log.memLog)
}

func deserializeLog(buf []byte) *logInMemory {
    var log logInMemory
    if err := utility.GobDecode(buf, &log); err != nil {
        return nil
    }
    return &log
}

func (log *Log) markAsReplied(logEntry *LogEntry) error {
    return log.js.Write("Reply:" + string(logEntry.encodedHash()))
}

func (log *Log) needSync(logEntry *LogEntry) bool {
    return true
}

func (log *Log) needReply(logEntry *LogEntry) bool {
    return true
}

/*
   write value if it's an update
   guarantee: the log is either on disk or not on disk.
   This relies on the guarantee of logStorage.Append.
   If no error is returned, the log is no disk.
   If the log is an update, the value is also on disk and marked as locally available.

   Value at this point should be encrypted.
*/
func (log *Log) persist(logEntry *LogEntry) error {
    if update, ok := logEntry.Message.(*Update); ok {
        if update.value != nil {
            logDebug.Debugf("Value to persist: %X", update.value)
            if err := log.WriteValue(update.HashOfValue, update.value); err != nil {
                return logDebug.Error(err)
            }
            update.value = nil
        } else {
            if logEntry.NodeId == log.memLog.MyNodeId {
                logDebug.Panicf("This should not happen: updates from me must have value with it.\n")
            } else {
                // TODO try to fetch value from remote in some situations? If the value is not persisted enough, needs to persist the value.
                // Right now the assumption is an upper layer will handle this.
            }
        }
    }
    return log.ls.Append(logEntry)
}

/*
   perform the sanity check of the log entry
   guarantee: if no error is returned, the log entry is ready to be committed. otherwise, the log entry should not be committed.
   After the log is committed, it is still possible to be a fork.
*/
func (log *Log) check(logEntry *LogEntry) error {
    if log.HasLogEntry(logEntry.NodeId, logEntry.encodedHash()) {
        return logDebug.Error(errors.New("Already seen this update."))
    }
    // if the issuer is blocked, the log entry should not be committed.
    if _, ok := log.memLog.BlackList[logEntry.NodeId]; ok {
        return logDebug.Error(errors.New("Cannot accept log from node that is blocked."))
    }

    // Check 1 Valid Signature.
    if err := logEntry.validateSignature(log.memLog.PublicKeys[logEntry.NodeId]); err != nil {
        return logDebug.Error(err)
    }

    // Check 3. Check if all updates this update depends on have been seen and check 4.
    for dependentNodeId, dependentVersionInfo := range logEntry.DVV {
        //if an update depends on a fork, there should be an I-vouch-for-this.
        dependentEncodedHash := dependentVersionInfo.HashOfUpdate
        if dependentLogEntry, ok := log.memLog.LogIndexedByHash[dependentEncodedHash]; !ok {
            logDebug.Debugf("Current dependent log: %v", dependentVersionInfo)
            logDebug.Debugf("Current log: %v", logEntry)
            return logDebug.Error(errors.New("Dependency not satisfied."))
        } else {
            if dependentNodeId != dependentLogEntry.NodeId {
                // TODO Write a test case to bypass this check.
                return logDebug.Error(errors.New("Wrong information. A node claim to depend on A from node B, but A isn't from B."))
            }
        }
    }

    // Check 5 Check against timestamp exhaust attack.
    if int64(logEntry.AcceptStamp) > 1000*time.Now().Unix() {
        return logDebug.Error(errors.New("Invalid accept stamp."))
    }
    // Check 6 type specific check
    if err := logEntry.Message.check(log, logEntry); err != nil {
        return logDebug.Error(err)
    }
    // Check 2 has to be newer than any existing updates.
    // if version vector says the update is observed,  a fork exists.
    // If the this logEntry doesn't depends on the latest updates from its author, that's a fork.
    _, lastLogEntry := log.getLastEntryOfNode(logEntry.NodeId)
    // a test case exists to capture this case
    if log.Observed(logEntry.NodeId, logEntry.AcceptStamp) {
        logDebug.Debugf("This is just a heuristic to detect.")
    }
    // An accpet stamp larger than latest update, but still is a fork.
    if previousVersionInfo, ok := logEntry.DVV[logEntry.NodeId]; ok {
        if lastLogEntry.encodedHash() != previousVersionInfo.HashOfUpdate {
            // Join fork here.
            logDebug.Debugf("Try to join the fork.")
            log.detectAndJoinFork(logEntry)
        }
    }
    return nil
}

// need in recovery
// join fork must have been done.
func (log *Log) updateMemoryState(logEntry *LogEntry) {
    // TODO write test cases. Write down what needs to be updated.
    encodedHashOfLogEntry := logEntry.encodedHash()
    virtualNodeId, ok := log.memLog.EntryNodeMap[encodedHashOfLogEntry]

    if !ok {
        virtualNodeId = logEntry.NodeId
        log.memLog.EntryNodeMap[encodedHashOfLogEntry] = virtualNodeId
    }
    // Previous log entry is guaranteed to have been seen or it is the first entry.
    log.memLog.SequentialLog[virtualNodeId] = append(log.memLog.SequentialLog[virtualNodeId], logEntry)
    // Put logEntry into index
    log.memLog.LogIndexedByHash[encodedHashOfLogEntry] = logEntry
    // find the node id by hash of the log entry
    // Update DVV
    currentVersion := versionInfo{
        logEntry.AcceptStamp,
        encodedHashOfLogEntry,
    }
    // Update version vector
    log.memLog.VersionVector[virtualNodeId] = currentVersion
    // update logic clock and dvv
    if logEntry.NodeId == log.memLog.MyNodeId {
        log.memLog.LogicalClock++
        log.memLog.DVV = versionVector{virtualNodeId: currentVersion}
    } else {
        if logEntry.AcceptStamp >= log.memLog.LogicalClock {
            log.memLog.LogicalClock = logEntry.AcceptStamp + 1
        }
        log.memLog.DVV[virtualNodeId] = currentVersion
    }
    logEntry.Message.handle(log, logEntry)
}

func newPOM(nodeId NodeID, hash1, hash2 EncodedHash) *pOM {
    pom := pOM{
        nodeId,
        hash1,
        hash2,
    }
    return &pom
}
