package logex

import (
    "fmt"
    . "launchpad.net/gocheck"
    "math/rand"
    "teapot/conf"
    "teapot/log"
    "teapot/utility"
    "testing"
    "time"
)

func Test(t *testing.T) { TestingT(t) }

type LogExSuite struct {
    dir        string
    n          int
    logEx      []*LogEx
    logEntries []*log.LogEntry
}

type FaultyLogExSuite struct {
    dir        string
    n          int
    logEx      []*LogEx
    logEntries []*log.LogEntry
}

var _ = Suite(&LogExSuite{})
var _ = Suite(&FaultyLogExSuite{})

func (s *LogExSuite) SetUpSuite(c *C) {
    rand.Seed(time.Now().Unix())
    s.dir = c.MkDir()
    s.n = 3
}

func (s *LogExSuite) TearDownSuite(c *C) {
}

func (s *LogExSuite) SetUpTest(c *C) {
    s.logEx = make([]*LogEx, 0)
    configs := conf.LoadMultipleTest(s.dir, s.n)
    for _, config := range configs {
        log := log.NewLog(config)
        logEx := NewLogEx(config, log)
        //logEx.startP2P(config.IPPort)
        s.logEx = append(s.logEx, logEx)
    }
    s.logEntries = nil
}

func (s *LogExSuite) TearDownTest(c *C) {
    for _, logEx := range s.logEx {
        logEx.stopP2P()
    }
}

func (s *FaultyLogExSuite) SetUpSuite(c *C) {
    rand.Seed(time.Now().Unix())
    s.dir = c.MkDir()
    s.n = 3
}

func (s *FaultyLogExSuite) TearDownSuite(c *C) {
}

type fakeP2PLogEx p2pLogEx

func (fp2p *fakeP2PLogEx) GetEntryByEncodedHash(logEx *LogEx, key log.EncodedHash, logEntry *log.LogEntry) error {
    p2p := p2pLogEx(*fp2p)
    return (&p2p).GetEntryByEncodedHash(logEx, key, logEntry)
}

func (fp2p *fakeP2PLogEx) GetLastLogEntryInfoOfNode(logEx *LogEx, nodeId log.NodeID, response *log.VersionInfo) error {
    p2p := p2pLogEx(*fp2p)
    return (&p2p).GetLastLogEntryInfoOfNode(logEx, nodeId, response)
}

func (fp2p *fakeP2PLogEx) GetValue(logEx *LogEx, key log.EncodedHash, object *[]byte) error {
    *object = []byte("faulty_world")
    return nil
}

func (s *FaultyLogExSuite) SetUpTest(c *C) {
    s.logEx = make([]*LogEx, 0)
    s.logEntries = make([]*log.LogEntry, 0)
    configs := conf.LoadMultipleTest(s.dir, s.n)
    for _, config := range configs {
        log := log.NewLog(config)
        logEx := NewLogEx(config, log)
        logEx.p2p = &fakeP2PLogEx{}
        //logEx.startP2P(config.IPPort)
        s.logEx = append(s.logEx, logEx)
    }
    for _, logEx := range s.logEx {
        update := logEx.theLog.NewUpdate(log.Key(string(logEx.myNodeId)+"/hello/0"), []byte("world"))
        logEntry := logEx.theLog.NewLogEntry(update)
        c.Assert(logEx.theLog.Commit(logEntry), IsNil)
        s.logEntries = append(s.logEntries, logEntry)
    }
}

func (s *FaultyLogExSuite) TearDownTest(c *C) {
    for _, logEx := range s.logEx {
        logEx.stopP2P()
    }
}

func (s *LogExSuite) TestBasicAntiEntropy(c *C) {
    for _, logEx := range s.logEx {
        update := logEx.theLog.NewUpdate(log.Key(string(logEx.myNodeId)+"/hello/0"), []byte("world"))
        logEntry := logEx.theLog.NewLogEntry(update)
        s.logEntries = append(s.logEntries, logEntry)
        c.Assert(logEx.theLog.Commit(logEntry), IsNil)
    }
    // Try to incorporate another node's update
    for i, logEx := range s.logEx {
        nextIndex := (i + 1) % s.n
        versionInfo, err := logEx.p2pAnyNewLogEntriesOfNode(s.logEx[nextIndex].myNodeId)
        err = logEx.antiEntropy(s.logEx[nextIndex].myNodeId, *versionInfo)
        c.Assert(err, IsNil)
    }
    // make sure that values are correctly stored on every clients.
    for _, logEx := range s.logEx {
        logEntries, err := logEx.theLog.GetCheckpoint(log.Key(string(logEx.myNodeId) + "/hello/0"))
        c.Assert(err, IsNil)
        c.Assert(logEntries, NotNil)
        c.Assert(len(logEntries), Equals, 1)
        value, err := logEx.theLog.GetDecryptValue(logEntries[0].Message.(*log.Update), logEntries[0].DVV)
        c.Assert(err, IsNil)
        c.Assert(string(value), Equals, "world")
    }
}

func (s *LogExSuite) TestAntiEntropyNoUpdates(c *C) {
    versionInfo, err := s.logEx[0].p2pAnyNewLogEntriesOfNode(s.logEx[1].myNodeId)
    c.Assert(err, IsNil)
    c.Assert(versionInfo, IsNil)
}

func (s *LogExSuite) TestAntiEntropy(c *C) {
    for i := 0; i < 30; i++ {
        index := rand.Int() % s.n
        logEx := s.logEx[index]
        update := logEx.theLog.NewUpdate(log.Key(fmt.Sprintf("%v/hello/%v", logEx.myNodeId, rand.Int()%5)), []byte(fmt.Sprintf("world%v", rand.Int()%5)))
        logEntry := logEx.theLog.NewLogEntry(update)
        c.Assert(logEx.theLog.Commit(logEntry), IsNil)
    }
    for i, logEx := range s.logEx {
        nextIndex := (i + 1) % s.n
        versionInfo, err := logEx.p2pAnyNewLogEntriesOfNode(s.logEx[nextIndex].myNodeId)
        err = logEx.antiEntropy(s.logEx[nextIndex].myNodeId, *versionInfo)
        c.Assert(err, IsNil)
    }
}

func (s *LogExSuite) TestAntiEntropyOfDuplicateDependency(c *C) {
    commit := []int{0, 1, 0, 0}
    source := []int{1, 1, 0, 2}
    dest := []int{0, 1, 1, 0}
    for i := 0; i < len(commit); i++ {
        if commit[i] != -1 {
            logEx := s.logEx[commit[i]]
            update := logEx.theLog.NewUpdate(log.Key(fmt.Sprintf("%v/hello/%v", logEx.myNodeId, rand.Int()%5)), []byte(fmt.Sprintf("world%v", rand.Int()%5)))
            logEntry := logEx.theLog.NewLogEntry(update)
            c.Assert(logEx.theLog.Commit(logEntry), IsNil)
        }
        if source[i] != dest[i] {
            sLogEx := s.logEx[source[i]]
            dLogEx := s.logEx[dest[i]]
            versionInfo, err := sLogEx.p2pAnyNewLogEntriesOfNode(dLogEx.myNodeId)
            c.Assert(err, IsNil)
            if versionInfo != nil {
                err := sLogEx.antiEntropy(dLogEx.myNodeId, *versionInfo)
                c.Assert(err, IsNil)
            }
        }
    }
}

// TODO
func (s *LogExSuite) TestAntiEntropyOfDuplicateDependencyMustCheckDuplicacyWhenCommit(c *C) {
}

func (s *LogExSuite) TestAntiRandomEntropyWithChmod(c *C) {
    for i := 0; i < 30; i++ {
        index := rand.Int() % s.n
        logEx := s.logEx[index]
        if rand.Intn(10) == 0 {
            // issue chmod
            chmod := logEx.theLog.NewChangeMode("hello", utility.KeyFromPassphrase("password"), []log.NodeID{}, []log.NodeID{})
            logEntry := logEx.theLog.NewLogEntry(chmod)
            c.Assert(logEx.theLog.Commit(logEntry), IsNil)
        } else {
            // issue logEntry
            update := logEx.theLog.NewUpdate(log.Key(fmt.Sprintf("%v/hello/%v", logEx.myNodeId, rand.Int()%5)), []byte(fmt.Sprintf("world%v", rand.Int()%5)))
            logEntry := logEx.theLog.NewLogEntry(update)
            c.Assert(logEx.theLog.Commit(logEntry), IsNil)
        }
        peerIndex := rand.Int() % s.n
        if peerIndex == index {
            continue
        }
        peerLogEx := s.logEx[peerIndex]
        versionInfo, err := logEx.p2pAnyNewLogEntriesOfNode(peerLogEx.myNodeId)
        c.Assert(err, IsNil)
        if versionInfo != nil {
            err := logEx.antiEntropy(peerLogEx.myNodeId, *versionInfo)
            c.Assert(err, IsNil)
        }
    }

    for index := 0; index < s.n; index++ {
        logEx := s.logEx[index]
        for peerIndex := 0; peerIndex < s.n; peerIndex++ {
            if index != peerIndex {
                peerLogEx := s.logEx[peerIndex]
                versionInfo, err := logEx.p2pAnyNewLogEntriesOfNode(peerLogEx.myNodeId)
                c.Assert(err, IsNil)
                if versionInfo != nil {
                    err := logEx.antiEntropy(peerLogEx.myNodeId, *versionInfo)
                    c.Assert(err, IsNil)
                }
            }
        }
    }
    update := s.logEx[0].theLog.NewUpdate(log.Key(fmt.Sprintf("%v/hello/%v", s.logEx[0].myNodeId, 0)), []byte(fmt.Sprintf("world%v", 0)))
    logEntry := s.logEx[0].theLog.NewLogEntry(update)
    c.Assert(s.logEx[0].theLog.Commit(logEntry), IsNil)
    update = s.logEx[0].theLog.NewUpdate(log.Key(fmt.Sprintf("%v/hello/%v", s.logEx[1].myNodeId, 0)), []byte(fmt.Sprintf("world%v", 0)))
    logEntry = s.logEx[0].theLog.NewLogEntry(update)
    c.Assert(s.logEx[0].theLog.Commit(logEntry), ErrorMatches, "Write Access Denied.*")
    update = s.logEx[0].theLog.NewUpdate(log.Key(fmt.Sprintf("%v/hello/%v", s.logEx[2].myNodeId, 0)), []byte(fmt.Sprintf("world%v", 0)))
    logEntry = s.logEx[0].theLog.NewLogEntry(update)
    c.Assert(s.logEx[0].theLog.Commit(logEntry), ErrorMatches, "Write Access Denied.*")
}

func (s *LogExSuite) TestRandomAntiEntropy(c *C) {
    for i := 0; i < 30; i++ {
        index := rand.Int() % s.n
        logEx := s.logEx[index]
        update := logEx.theLog.NewUpdate(log.Key(fmt.Sprintf("%v/hello/%v", logEx.myNodeId, rand.Int()%5)), []byte(fmt.Sprintf("world%v", rand.Int()%5)))
        logEntry := logEx.theLog.NewLogEntry(update)
        c.Assert(logEx.theLog.Commit(logEntry), IsNil)
        var peerIndex int
        for {
            peerIndex = rand.Int() % s.n
            if peerIndex != index {
                break
            }
        }
        peerLogEx := s.logEx[peerIndex]
        versionInfo, err := logEx.p2pAnyNewLogEntriesOfNode(peerLogEx.myNodeId)
        c.Assert(err, IsNil)
        if versionInfo != nil {
            err := logEx.antiEntropy(peerLogEx.myNodeId, *versionInfo)
            c.Assert(err, IsNil)
        }
    }
}

// The p2p server is faked, so the returned data is corrupted.
func (s *FaultyLogExSuite) TestTampperedValueChecking(c *C) {
    // Try to incorporate another node's update
    for i, logEx := range s.logEx {
        nextIndex := (i + 1) % s.n
        versionInfo, err := logEx.p2pAnyNewLogEntriesOfNode(s.logEx[nextIndex].myNodeId)
        err = logEx.antiEntropy(s.logEx[nextIndex].myNodeId, *versionInfo)
        c.Assert(err, ErrorMatches, "Hash doesn't match.")
    }
}
