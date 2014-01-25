package log

import (
    "fmt"
    . "launchpad.net/gocheck"
    "strings"
    "teapot/conf"
    "teapot/utility"
)

type GCSuite struct {
    n   int
    dir string
}

var _ = Suite(&GCSuite{})

func (s *GCSuite) SetUpSuite(c *C) {
    s.n = 3
    s.dir = c.MkDir()
}

func (s *GCSuite) TestGCWithRestart(c *C) {
    configs := conf.LoadMultipleTest(s.dir, s.n)
    logs := make([]*Log, s.n)
    for i, config := range configs {
        logs[i] = NewLog(config)
    }
    for i := 0; i < 3; i++ {
        update := logs[0].NewUpdate(Key(fmt.Sprintf("%v/hello/%v", logs[0].memLog.MyNodeId, i%s.n)), []byte(fmt.Sprintf("world%v", i)))
        logEntry := logs[0].NewLogEntry(update)
        c.Assert(logs[0].Commit(logEntry), IsNil)
        c.Assert(logs[1].Commit(logEntry), IsNil)
        c.Assert(logs[2].Commit(logEntry), IsNil)
    }
    fs := faultySet{
        logs[0].memLog.BlackList,
    }
    fsLog := logs[0].NewLogEntry(&fs)
    c.Assert(logs[0].Commit(fsLog), IsNil)
    c.Assert(logs[0].AsyncHandle(fsLog), IsNil)
    ji := newJournalIterator(configs[0].JournalPath)
    c.Assert(ji, NotNil)
    entry, err := ji.NextJournalEntry()
    c.Assert(err, IsNil)
    c.Assert(strings.HasPrefix(entry, "Reply"), Equals, true)

    logs[0] = NewLog(configs[0])
    c.Assert(logs[1].Commit(fsLog), IsNil)
    c.Assert(logs[1].AsyncHandle(fsLog), IsNil)
    c.Assert(logs[2].Commit(fsLog), IsNil)
    c.Assert(logs[2].AsyncHandle(fsLog), IsNil)
    _, ackLog := logs[0].getLastEntryOfNode(logs[0].memLog.MyNodeId)
    c.Assert(logs[1].Commit(ackLog), IsNil)
    c.Assert(logs[2].Commit(ackLog), IsNil)
    _, ackLog = logs[1].getLastEntryOfNode(logs[1].memLog.MyNodeId)
    c.Assert(logs[0].Commit(ackLog), IsNil)
    c.Assert(len(logs[0].memLog.AckFaultySet), Equals, 1)
    c.Assert(logs[0].AsyncHandle(ackLog), IsNil)
    c.Assert(logs[2].Commit(ackLog), IsNil)
    _, ackLog = logs[2].getLastEntryOfNode(logs[2].memLog.MyNodeId)
    c.Assert(logs[0].Commit(ackLog), IsNil)
    c.Assert(len(logs[0].memLog.AckFaultySet), Equals, 1)
    c.Assert(logs[0].AsyncHandle(ackLog), IsNil)
    c.Assert(logs[1].Commit(ackLog), IsNil)
    c.Assert(len(logs[0].memLog.AckFaultySet), Equals, 0)
    // every node acked. Now CDL issued
    _, cdlLog := logs[0].getLastEntryOfNode(logs[0].memLog.MyNodeId)
    logs[0] = NewLog(configs[0])
    c.Assert(logs[1].Commit(cdlLog), IsNil)
    c.Assert(logs[1].AsyncHandle(cdlLog), IsNil)
    c.Assert(len(logs[1].memLog.LocalCDLs), Equals, 1)
    c.Assert(logs[2].Commit(cdlLog), IsNil)
    c.Assert(logs[2].AsyncHandle(cdlLog), IsNil)
    c.Assert(len(logs[2].memLog.LocalCDLs), Equals, 1)

    logs[1] = NewLog(configs[1])
    logs[2] = NewLog(configs[2])
    _, cdlLog = logs[1].getLastEntryOfNode(logs[1].memLog.MyNodeId)
    c.Assert(logs[0].Commit(cdlLog), IsNil)
    c.Assert(logs[0].AsyncHandle(cdlLog), IsNil)
    c.Assert(len(logs[0].memLog.LocalCDLs), Equals, 1)
    c.Assert(logs[2].Commit(cdlLog), IsNil)
    c.Assert(logs[2].AsyncHandle(cdlLog), IsNil)
    c.Assert(len(logs[2].memLog.LocalCDLs), Equals, 0)

    _, cdlLog = logs[2].getLastEntryOfNode(logs[2].memLog.MyNodeId)
    c.Assert(logs[0].Commit(cdlLog), IsNil)
    c.Assert(logs[0].AsyncHandle(cdlLog), IsNil)
    c.Assert(len(logs[0].memLog.LocalCDLs), Equals, 0)
    c.Assert(logs[1].Commit(cdlLog), IsNil)
    c.Assert(logs[1].AsyncHandle(cdlLog), IsNil)
    c.Assert(len(logs[1].memLog.LocalCDLs), Equals, 0)
    logs[0] = NewLog(configs[0])
    // TODO fix this
    //c.Assert(len(logs[0].memLog.LocalCDLs), Equals, 0)
}

func (s *GCSuite) TestGCWithAccessControl(c *C) {
    config := conf.LoadTest(s.dir, 0)
    log := NewLog(config)
    // write each key with 3 updates.
    for i := 0; i < 3*s.n; i++ {
        update := log.NewUpdate(Key(fmt.Sprintf("%v/hello/%v", log.memLog.MyNodeId, i%s.n)), []byte(fmt.Sprintf("world%v", i)))
        logEntry := log.NewLogEntry(update)
        c.Assert(log.Commit(logEntry), IsNil)
    }
    // chmod
    chmod := log.NewChangeMode(Dir("hello"), SecretKey(utility.KeyFromPassphrase("password")), []NodeID{}, []NodeID{})
    logEntry := log.NewLogEntry(chmod)
    c.Assert(log.Commit(logEntry), IsNil)
    // write each key with another 3 updates.
    for i := 3*s.n + 1; i < 5*s.n; i++ {
        update := log.NewUpdate(Key(fmt.Sprintf("%v/hello/%v", log.memLog.MyNodeId, i%s.n)), []byte(fmt.Sprintf("world%v", i)))
        logEntry := log.NewLogEntry(update)
        c.Assert(log.Commit(logEntry), IsNil)
    }
    // garbage collection
    c.Assert(log.GC(), IsNil)
    c.Assert(len(log.memLog.SequentialLog[log.memLog.MyNodeId]), Equals, 3)
    // read each of the 3 keys to get the old value.
    for i := 0; i < s.n; i++ {
        logEntries, err := log.GetCheckpoint(Key(fmt.Sprintf("%v/hello/%v", log.memLog.MyNodeId, i%s.n)))
        c.Assert(err, IsNil)
        c.Assert(len(logEntries), Equals, 1)
        logEntry := logEntries[0]
        update := logEntry.Message.(*Update)
        value, err := log.GetDecryptValue(update, logEntry.DVV)
        c.Assert(err, IsNil)
        c.Assert(value, DeepEquals, []byte(fmt.Sprintf("world%v", 4*s.n+i)))
    }
    for i := 5*s.n + 1; i < 8*s.n; i++ {
        update := log.NewUpdate(Key(fmt.Sprintf("%v/hello/%v", log.memLog.MyNodeId, i%s.n)), []byte(fmt.Sprintf("world%v", i)))
        logEntry := log.NewLogEntry(update)
        c.Assert(log.Commit(logEntry), IsNil)
    }
    chmod = log.NewChangeMode(Dir("hello"), SecretKey(utility.KeyFromPassphrase("password1")), []NodeID{}, []NodeID{})
    logEntry = log.NewLogEntry(chmod)
    c.Assert(log.Commit(logEntry), IsNil)
    for i := 8*s.n + 1; i < 10*s.n; i++ {
        update := log.NewUpdate(Key(fmt.Sprintf("%v/hello/%v", log.memLog.MyNodeId, i%s.n)), []byte(fmt.Sprintf("world%v", i)))
        logEntry := log.NewLogEntry(update)
        c.Assert(log.Commit(logEntry), IsNil)
    }
    // garbage collection again.
    c.Assert(log.GC(), IsNil)
    c.Assert(len(log.memLog.SequentialLog[log.memLog.MyNodeId]), Equals, 3)
    for i := 0; i < s.n; i++ {
        logEntries, err := log.GetCheckpoint(Key(fmt.Sprintf("%v/hello/%v", log.memLog.MyNodeId, i%s.n)))
        c.Assert(err, IsNil)
        c.Assert(len(logEntries), Equals, 1)
        logEntry := logEntries[0]
        update := logEntry.Message.(*Update)
        value, err := log.GetDecryptValue(update, logEntry.DVV)
        c.Assert(err, IsNil)
        c.Assert(value, DeepEquals, []byte(fmt.Sprintf("world%v", 9*s.n+i)))
    }
    for i := 10*s.n + 1; i < 12*s.n; i++ {
        update := log.NewUpdate(Key(fmt.Sprintf("%v/hello/%v", log.memLog.MyNodeId, i%s.n)), []byte(fmt.Sprintf("world%v", i)))
        logEntry := log.NewLogEntry(update)
        c.Assert(log.Commit(logEntry), IsNil)
    }
    for i := 0; i < s.n; i++ {
        logEntries, err := log.GetCheckpoint(Key(fmt.Sprintf("%v/hello/%v", log.memLog.MyNodeId, i%s.n)))
        c.Assert(err, IsNil)
        c.Assert(len(logEntries), Equals, 1)
        logEntry := logEntries[0]
        update := logEntry.Message.(*Update)
        value, err := log.GetDecryptValue(update, logEntry.DVV)
        c.Assert(err, IsNil)
        c.Assert(value, DeepEquals, []byte(fmt.Sprintf("world%v", 11*s.n+i)))
    }
    for i := 0; i < s.n; i++ {
        logEntries, err := log.GetCheckpointVersion(Key(fmt.Sprintf("%v/hello/%v", log.memLog.MyNodeId, i%s.n)), 0)
        c.Assert(err, IsNil)
        c.Assert(len(logEntries), Equals, 1)
        logEntry := logEntries[0]
        update := logEntry.Message.(*Update)
        value, err := log.GetDecryptValue(update, logEntry.DVV)
        c.Assert(err, IsNil)
        c.Assert(value, DeepEquals, []byte(fmt.Sprintf("world%v", 4*s.n+i)))
    }
    for i := 0; i < s.n; i++ {
        logEntries, err := log.GetCheckpointVersion(Key(fmt.Sprintf("%v/hello/%v", log.memLog.MyNodeId, i%s.n)), 1)
        c.Assert(err, IsNil)
        c.Assert(len(logEntries), Equals, 1)
        logEntry := logEntries[0]
        update := logEntry.Message.(*Update)
        value, err := log.GetDecryptValue(update, logEntry.DVV)
        c.Assert(err, IsNil)
        c.Assert(value, DeepEquals, []byte(fmt.Sprintf("world%v", 9*s.n+i)))
    }
}
