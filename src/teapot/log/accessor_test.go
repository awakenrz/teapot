package log

import (
    . "launchpad.net/gocheck"
    "os"
    "strconv"
    "teapot/conf"
)

type AccessorSuite struct {
    dir string
}

var _ = Suite(&AccessorSuite{})

func (s *AccessorSuite) SetUpSuite(c *C) {
    s.dir = c.MkDir()
}

func (s *AccessorSuite) TearDownSuite(c *C) {
    os.RemoveAll(s.dir)
}

func (s *AccessorSuite) TestGetEntryByAcceptstamp(c *C) {
    config := conf.LoadTest(s.dir, 0)
    log := newTestableLog(config)
    for i := 1; i < 10; i++ {
        update := log.NewUpdate(Key(string(log.memLog.MyNodeId)+"/dir/"+strconv.Itoa(i)), []byte(strconv.Itoa(i)))
        logEntry := log.NewLogEntry(update)
        c.Assert(log.Commit(logEntry), IsNil)
        log.memLog.LogicalClock++
        index, logEntry := log.getEntryByAcceptstamp(log.memLog.MyNodeId, Timestamp(i))
        if i%2 == 0 {
            c.Assert(logEntry, IsNil)
            c.Assert(index, Equals, -1)
        } else {
            c.Assert(logEntry, NotNil)
            c.Assert(index, Equals, (i-1)/2)
        }
    }
}

func (s *AccessorSuite) TestGetLastEntry(c *C) {
    config := conf.LoadTest(s.dir, 0)
    log := newTestableLog(config)
    for i := 1; i < 10; i++ {
        update := log.NewUpdate(Key(string(log.memLog.MyNodeId)+"/dir/"+strconv.Itoa(i)), []byte(strconv.Itoa(i)))
        logEntry := log.NewLogEntry(update)
        c.Assert(log.Commit(logEntry), IsNil)
        index, logEntry := log.getLastEntryOfNode(log.memLog.MyNodeId)
        c.Assert(index, Equals, i-1)
        c.Assert(logEntry, NotNil)
        c.Assert(logEntry.AcceptStamp, Equals, Timestamp(i))
        update, ok := logEntry.Message.(*Update)
        c.Assert(ok, Equals, true)
        c.Assert(update.Key, Equals, Key(string(log.memLog.MyNodeId)+"/dir/"+strconv.Itoa(i)))
    }
}

func (s *AccessorSuite) TestGetIthEntry(c *C) {
    config := conf.LoadTest(s.dir, 0)
    log := newTestableLog(config)
    for i := 1; i < 10; i++ {
        update := log.NewUpdate(Key(string(log.memLog.MyNodeId)+"/dir/"+strconv.Itoa(i)), []byte(strconv.Itoa(i)))
        logEntry := log.NewLogEntry(update)
        c.Assert(log.Commit(logEntry), IsNil)
        logEntry = log.getIthEntryOfNode(log.memLog.MyNodeId, 5)
        if i > 5 {
            c.Assert(logEntry, NotNil)
            update, ok := logEntry.Message.(*Update)
            c.Assert(ok, Equals, true)
            c.Assert(update.Key, Equals, Key(log.memLog.MyNodeId+"/dir/6"))
        } else {
            c.Assert(logEntry, IsNil)
        }
    }
}
