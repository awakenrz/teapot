package logex

import (
    . "launchpad.net/gocheck"
    "teapot/conf"
    "teapot/log"
)

type P2PSuite struct {
    dir        string
    n          int
    logEx      []*LogEx
    logEntries []*log.LogEntry
}

var _ = Suite(&P2PSuite{})

func (s *P2PSuite) SetUpSuite(c *C) {
    s.dir = c.MkDir()
    s.n = 3
}

func (s *P2PSuite) TearDownSuite(c *C) {
}

func (s *P2PSuite) SetUpTest(c *C) {
    s.logEx = make([]*LogEx, 0)
    s.logEntries = make([]*log.LogEntry, 0)
    configs := conf.LoadMultipleTest(s.dir, s.n)
    for _, config := range configs {
        log := log.NewLog(config)
        logEx := NewLogEx(config, log)
        s.logEx = append(s.logEx, logEx)
    }
    /*    for _, logEx := range s.logEx {
          update := logEx.theLog.NewUpdate(log.Key(string(logEx.myNodeId) +"/hello/0"), []byte("world"))
          logEntry := logEx.theLog.NewLogEntry(update)
          s.logEntries = append(s.logEntries, logEntry)
          c.Assert(logEx.theLog.Commit(logEntry), IsNil)
      }*/
}

func (s *P2PSuite) TearDownTest(c *C) {
    for _, logEx := range s.logEx {
        logEx.stopP2P()
    }
}

func (s *P2PSuite) TestP2P(c *C) {
    for _, logEx := range s.logEx {
        update := logEx.theLog.NewUpdate(log.Key(string(logEx.myNodeId)+"/hello/0"), []byte("world"))
        logEntry := logEx.theLog.NewLogEntry(update)
        s.logEntries = append(s.logEntries, logEntry)
        c.Assert(logEx.theLog.Commit(logEntry), IsNil)
    }
    for i, logEx := range s.logEx {
        nextIndex := (i + 1) % s.n
        versionInfo, err := logEx.p2pAnyNewLogEntriesOfNode(s.logEx[nextIndex].myNodeId)
        c.Assert(err, IsNil)
        c.Assert(versionInfo, NotNil)
        c.Assert(versionInfo.AcceptStamp, Equals, log.Timestamp(1))
        c.Logf("%v", s.logEntries[nextIndex])
        c.Assert(versionInfo.HashOfUpdate, Equals, s.logEntries[nextIndex].EncodedHash())
        logEntry, err := logEx.p2pGetEntryByEncodedHash(s.logEx[nextIndex].myNodeId, versionInfo.HashOfUpdate)
        c.Assert(err, IsNil)
        c.Assert(logEntry, DeepEquals, s.logEntries[nextIndex])
        update, ok := logEntry.Message.(*log.Update)
        c.Assert(ok, Equals, true)
        value, err := logEx.p2pGetValue(logEntry.NodeId, update.HashOfValue)
        c.Assert(err, IsNil)
        c.Assert(value, NotNil)
        // TODO
        //c.Assert(string(value), Equals, "world")
    }
}
