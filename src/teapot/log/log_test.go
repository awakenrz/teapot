package log

import (
    "crypto/rsa"
    "fmt"
    . "launchpad.net/gocheck"
    "math/rand"
    "sync"
    "teapot/conf"
    "testing"
)

func Test(t *testing.T) { TestingT(t) }

type SingleLogSuite struct {
    dir string
    log *Log
}

type CheckSuite struct {
    n    int
    dir  string
    logs []*Log
}

type LogSuite struct {
    dir string
}

var _ = Suite(&SingleLogSuite{})
var _ = Suite(&CheckSuite{})
var _ = Suite(&LogSuite{})

type fakeStorage struct {
}

func (fs *fakeStorage) Append(logEntries *LogEntry) error {
    return nil
}
func (fs *fakeStorage) Write(entry string) error {
    return nil
}

func (fs *fakeStorage) NextLogEntry() (*LogEntry, error) {
    return nil, nil
}

func (fs *fakeStorage) NextJournalEntry() (string, error) {
    return "", nil
}

func (fs *fakeStorage) WriteValue(encodedHash EncodedHash, value []byte) error {
    return nil
}

func (fs *fakeStorage) ReadValue(encodedHash EncodedHash) ([]byte, error) {
    return nil, nil
}

func (fs *fakeStorage) GetAllSnapshotFolders() ([]string, error) {
    return nil, nil
}

func (fs *fakeStorage) GetLastSnapshotFolder() (string, error) {
    return "", nil
}

func (fs *fakeStorage) NewSnapshot(log *logInMemory) (string, error) {
    return "", nil
}

func (fs *fakeStorage) ReadSnapshot(folder string) (*logInMemory, error) {
    return nil, nil
}

func (fs *fakeStorage) SyncLogEntry(logEntries *LogEntry) {
}

func (fs *fakeStorage) StartSyncLog() {
}

func (fs *fakeStorage) GetValue(encodedHash EncodedHash) ([]byte, error) {
    return nil, nil
}

func newTestableLog(config *conf.Config) *Log {
    publicKeys := make(map[NodeID]*rsa.PublicKey)
    for nodeId, publicKey := range config.PublicKeys {
        publicKeys[NodeID(nodeId)] = publicKey
    }
    fs := &fakeStorage{}

    log := Log{
        newLogInMemory(config),

        config,
        new(sync.Mutex),
        new(sync.Mutex),

        fs,
        fs,
        fs,
        fs,
        fs,
    }
    return &log
}

func (s *SingleLogSuite) SetUpSuite(c *C) {
    s.dir = c.MkDir()
}

func (s *SingleLogSuite) TearDownSuite(c *C) {
}

func (s *SingleLogSuite) SetUpTest(c *C) {
    config := conf.LoadTest(s.dir, 0)
    s.log = newTestableLog(config)
}

func (s *SingleLogSuite) TearDownTest(c *C) {
}

func (s *CheckSuite) SetUpSuite(c *C) {
    s.dir = c.MkDir()
}

func (s *CheckSuite) TearDownSuite(c *C) {
}

func (s *CheckSuite) SetUpTest(c *C) {
    n := 3
    s.n = n
    configs := conf.LoadMultipleTest(s.dir, n)
    c.Assert(len(configs), Equals, n)
    s.logs = make([]*Log, n, n)
    for i := 0; i < n; i++ {
        log := newTestableLog(configs[i])
        c.Assert(log, NotNil)
        s.logs[i] = log
    }
}

func (s *CheckSuite) TearDownTest(c *C) {
}

func (s *LogSuite) SetUpSuite(c *C) {
    s.dir = c.MkDir()
}

func (s *SingleLogSuite) TestSerialization(c *C) {
    compareLog(c, s.log.memLog, deserializeLog(s.log.serialize()))
}

func (s *SingleLogSuite) TestCommitLog(c *C) {
    update := s.log.NewUpdate("abc/hello/1", []byte("world"))
    logEntry := s.log.NewLogEntry(update)
    c.Assert(s.log.Commit(logEntry), ErrorMatches, "Write Access Denied.*")
    update = s.log.NewUpdate(Key(string(s.log.memLog.MyNodeId)+"/hello/2"), []byte("world"))
    logEntry = s.log.NewLogEntry(update)
    c.Assert(s.log.Commit(logEntry), IsNil)
}

func (s *CheckSuite) TestCheck1(c *C) {
    logEntries := make([]*LogEntry, 0)
    for _, log := range s.logs {
        update := log.NewUpdate(Key(string(log.memLog.MyNodeId)+"/hello/0"), []byte("world"))
        logEntries = append(logEntries, log.NewLogEntry(update))
    }

    // commit order 1.
    c.Assert(s.logs[0].Commit(logEntries[0]), IsNil)
    c.Assert(s.logs[1].Commit(logEntries[1]), IsNil)
    c.Assert(s.logs[0].Commit(logEntries[1]), IsNil)
    c.Assert(s.logs[1].Commit(logEntries[0]), IsNil)
}

func (s *CheckSuite) TestCheck2(c *C) {
    logEntries := make([]*LogEntry, 0)
    for _, log := range s.logs {
        update := log.NewUpdate(Key(string(log.memLog.MyNodeId)+"/hello/0"), []byte("world"))
        logEntries = append(logEntries, log.NewLogEntry(update))
    }

    // commit order 2.
    c.Assert(s.logs[0].Commit(logEntries[0]), IsNil)
    c.Assert(s.logs[1].Commit(logEntries[0]), IsNil)
    c.Assert(s.logs[1].Commit(logEntries[1]), IsNil)
    c.Assert(s.logs[0].Commit(logEntries[1]), IsNil)

    c.Assert(logEntries[0].encodedHash(), Equals, logEntries[1].DVV[s.logs[0].memLog.MyNodeId].HashOfUpdate)
    c.Assert(logEntries[1].AcceptStamp, Equals, Timestamp(2))
}

func (s *CheckSuite) TestRandomCommitCheck(c *C) {
    for i := 0; i < 100; i++ {
        index := rand.Int() % s.n
        log := s.logs[index]
        update := log.NewUpdate(Key(fmt.Sprintf("%v/hello/%v", log.memLog.MyNodeId, rand.Int()%5)), []byte(fmt.Sprintf("world%v", rand.Int()%5)))
        logEntry := log.NewLogEntry(update)
        c.Assert(log.Commit(logEntry), IsNil)
    }
}

func (s *CheckSuite) TestCheckAlreadySeen(c *C) {
    logEntries := make([]*LogEntry, 0)
    for _, log := range s.logs {
        update := log.NewUpdate(Key(string(log.memLog.MyNodeId)+"/hello/0"), []byte("world"))
        logEntries = append(logEntries, log.NewLogEntry(update))
    }

    // commit order 2.
    c.Assert(s.logs[0].Commit(logEntries[0]), IsNil)
    c.Assert(s.logs[1].Commit(logEntries[0]), IsNil)
    c.Assert(s.logs[1].Commit(logEntries[1]), IsNil)
    c.Assert(s.logs[0].Commit(logEntries[1]), IsNil)

    c.Assert(s.logs[0].Commit(logEntries[1]), ErrorMatches, "Already seen this update.")
}

func (s *CheckSuite) TestCheckBadSignature(c *C) {
    logEntries := make([]*LogEntry, 0)
    for _, log := range s.logs {
        update := log.NewUpdate(Key(string(log.memLog.MyNodeId)+"/hello/0"), []byte("world"))
        logEntries = append(logEntries, log.NewLogEntry(update))
    }

    // commit order 2.
    c.Assert(s.logs[0].Commit(logEntries[0]), IsNil)
    c.Assert(s.logs[1].Commit(logEntries[0]), IsNil)
    c.Assert(s.logs[1].Commit(logEntries[1]), IsNil)
    logEntries[1].Sig = make([]byte, 10, 10)
    c.Assert(s.logs[0].Commit(logEntries[1]), ErrorMatches, ".*verification error.*")
}

func (s *CheckSuite) TestCheckHistory(c *C) {
    logEntries := make([]*LogEntry, 0)
    for _, log := range s.logs {
        update := log.NewUpdate(Key(string(log.memLog.MyNodeId)+"/hello/0"), []byte("world"))
        logEntries = append(logEntries, log.NewLogEntry(update))
    }

    // commit order 2.
    c.Assert(s.logs[0].Commit(logEntries[0]), IsNil)
    c.Assert(s.logs[1].Commit(logEntries[0]), IsNil)
    c.Assert(s.logs[1].Commit(logEntries[1]), IsNil)
    c.Assert(s.logs[0].Commit(logEntries[1]), IsNil)
    c.Assert(s.logs[2].Commit(logEntries[1]), ErrorMatches, "Dependency not satisfied.")
    c.Assert(s.logs[2].Commit(logEntries[0]), IsNil)
    c.Assert(s.logs[2].Commit(logEntries[2]), IsNil)
    logEntries[2].DVV[s.logs[1].memLog.MyNodeId] = logEntries[2].DVV[s.logs[0].memLog.MyNodeId]
    delete(logEntries[2].DVV, s.logs[0].memLog.MyNodeId)
    logEntries[2].sign(s.logs[2].memLog.PrivateKey)
    c.Assert(s.logs[0].Commit(logEntries[2]), ErrorMatches, "Wrong information.*")
    c.Assert(s.logs[1].Commit(logEntries[2]), ErrorMatches, "Wrong information.*")
}

func (s *CheckSuite) TestCheckTimestamp(c *C) {
    logEntries := make([]*LogEntry, 0)
    for _, log := range s.logs {
        update := log.NewUpdate(Key(string(log.memLog.MyNodeId)+"/hello/0"), []byte("world"))
        logEntries = append(logEntries, log.NewLogEntry(update))
    }

    // commit order 2.
    c.Assert(s.logs[0].Commit(logEntries[0]), IsNil)
    c.Assert(s.logs[1].Commit(logEntries[0]), IsNil)
    c.Assert(s.logs[1].Commit(logEntries[1]), IsNil)
    c.Assert(s.logs[0].Commit(logEntries[1]), IsNil)
    c.Assert(s.logs[2].Commit(logEntries[2]), IsNil)
    logEntries[2].AcceptStamp = 2147483648000
    logEntries[2].sign(s.logs[2].memLog.PrivateKey)
    c.Assert(s.logs[0].Commit(logEntries[2]), ErrorMatches, "Invalid accept stamp.")
}

func (s *CheckSuite) TestCheckAndJoinFork(c *C) {
    logEntries := make([]*LogEntry, 0)
    for _, log := range s.logs {
        update := log.NewUpdate(Key(string(log.memLog.MyNodeId)+"/hello/0"), []byte("world"))
        logEntries = append(logEntries, log.NewLogEntry(update))
    }

    // commit order 2.
    c.Assert(s.logs[0].Commit(logEntries[0]), IsNil)
    c.Assert(s.logs[1].Commit(logEntries[0]), IsNil)
    c.Assert(s.logs[1].Commit(logEntries[1]), IsNil)
    c.Assert(s.logs[0].Commit(logEntries[1]), IsNil)

    update := s.logs[1].NewUpdate(Key(string(s.logs[1].memLog.MyNodeId)+"/hello/0"), []byte("world1"))
    logEntry := s.logs[1].NewLogEntry(update)
    c.Assert(s.logs[1].Commit(logEntry), IsNil)
    c.Assert(s.logs[0].Commit(logEntry), IsNil)
    newLogEntry := *logEntry
    newLogEntry.AcceptStamp += 10
    newLogEntry.sign(s.logs[1].memLog.PrivateKey)
    // This logEntry is a fork with the exising one. Will be joined.
    c.Assert(s.logs[0].Commit(&newLogEntry), IsNil)
    c.Assert(s.logs[0].memLog.BlackList[s.logs[1].memLog.MyNodeId], NotNil)
    virtualNodeId1 := NodeID(string(logEntry.NodeId) + "." + string(logEntry.encodedHash()))
    virtualNodeId2 := NodeID(string(newLogEntry.NodeId) + "." + string(newLogEntry.encodedHash()))
    c.Assert(s.logs[0].memLog.VersionVector[virtualNodeId1].HashOfUpdate, Equals, logEntry.encodedHash())
    c.Assert(s.logs[0].memLog.VersionVector[virtualNodeId2].HashOfUpdate, Equals, newLogEntry.encodedHash())
    c.Assert(s.logs[0].memLog.EntryNodeMap[logEntry.encodedHash()], Equals, virtualNodeId1)
    c.Assert(s.logs[0].memLog.EntryNodeMap[newLogEntry.encodedHash()], Equals, virtualNodeId2)
}

func (s *LogSuite) TestRealLog(c *C) {
    config := conf.LoadTest(s.dir, 0)
    log := NewLog(config)
    update := log.NewUpdate(Key(string(log.memLog.MyNodeId)+"/hello/2"), []byte("world"))
    logEntry := log.NewLogEntry(update)
    c.Assert(log.Commit(logEntry), IsNil)
}

func (s *LogSuite) TestDetectingTampperedValue(c *C) {
    /*    config := conf.LoadTest(s.dir, 0)
          log := NewLog(config)
          update := log.NewUpdate(Key(string(log.memLog.MyNodeId) +"/hello/2"), []byte("world"))
          log.tempValueStore[update.HashOfValue] = append(log.tempValueStore[update.HashOfValue], 125)
          logEntry := log.NewLogEntry(update)
          c.Assert(log.Commit(logEntry), ErrorMatches, "Hash doesn't match.")*/
}

// TODO DeepEqual should be fine.
func (s *LogSuite) TestReplayLog(c *C) {
    config := conf.LoadTest(s.dir, 0)
    log := NewLog(config)
    c.Assert(log, NotNil)
    update := log.NewUpdate(Key(string(log.memLog.MyNodeId)+"/hello/2"), []byte("world"))
    logEntry := log.NewLogEntry(update)
    c.Assert(log.Commit(logEntry), IsNil)

    replayLog := NewLog(config)
    c.Assert(replayLog, NotNil)
    compareLog(c, replayLog.memLog, log.memLog)
    // TODO put more
}

func compareLog(c *C, log1, log2 *logInMemory) {
    c.Assert(log1.MyNodeId, DeepEquals, log2.MyNodeId)
    //    c.Assert(log1.PrivateKey, DeepEquals, log2.PrivateKey)
    c.Assert(log1.PublicKeys, DeepEquals, log2.PublicKeys)

    c.Assert(log1.SequentialLog, DeepEquals, log2.SequentialLog)
    c.Assert(log1.LogIndexedByHash, DeepEquals, log2.LogIndexedByHash)
    c.Assert(log1.EntryNodeMap, DeepEquals, log2.EntryNodeMap)
    c.Assert(log1.DVV, DeepEquals, log2.DVV)
    c.Assert(log1.VersionVector, DeepEquals, log2.VersionVector)

    c.Assert(log1.LogicalClock, DeepEquals, log2.LogicalClock)
    c.Assert(log1.Checkpoint, DeepEquals, log2.Checkpoint)
    c.Assert(log1.DefaultKey, DeepEquals, log2.DefaultKey)

    c.Assert(log1.ReadKeyInfo, DeepEquals, log2.ReadKeyInfo)
    c.Assert(log1.WriteKeyInfo, DeepEquals, log2.WriteKeyInfo)
    //    c.Assert(log1.Modes, DeepEquals, log2.Modes)

    c.Assert(log1.Values, DeepEquals, log2.Values)
    c.Assert(log1.LocalCDLs, DeepEquals, log2.LocalCDLs)
    c.Assert(log1.ProposedFaultySet, DeepEquals, log2.ProposedFaultySet)
    c.Assert(log1.AckFaultySet, DeepEquals, log2.AckFaultySet)
}

func (s *LogSuite) TestComplexReplayLog(c *C) {
}

func (s *LogSuite) TestReadWriteValue(c *C) {
    config := conf.LoadTest(s.dir, 0)
    log := NewLog(config)
    c.Assert(log, NotNil)
    update := log.NewUpdate(Key(string(log.memLog.MyNodeId)+"/hello/2"), []byte("world"))
    c.Assert(log.WriteValue(update.HashOfValue, update.value), ErrorMatches, "Hash doesn't match.")
    logEntry := log.NewLogEntry(update)
    c.Assert(log.Commit(logEntry), IsNil)
    c.Assert(log.WriteValue(update.HashOfValue, update.value), ErrorMatches, "Hash doesn't match.")
    value, err := log.GetValue(update.HashOfValue)
    c.Assert(err, IsNil)
    c.Assert(value, NotNil)
    value, err = log.GetDecryptValue(update, logEntry.DVV)
    c.Assert(err, IsNil)
    c.Assert(value, DeepEquals, []byte("world"))
}
