package log

import (
    . "launchpad.net/gocheck"
    "teapot/conf"
)

type LogEntrySuite struct {
    dir string
}

var _ = Suite(&LogEntrySuite{})

func (s *LogEntrySuite) SetUpSuite(c *C) {
    s.dir = c.MkDir()
}

func (s *LogEntrySuite) SetUpTest(c *C) {
}

func (s *LogEntrySuite) TestSignAndValidate(c *C) {
    config := conf.LoadTest(s.dir, 0)
    log := newTestableLog(config)
    update := log.NewUpdate(Key(string(log.memLog.MyNodeId)+"/hello/2"), []byte("world"))
    logEntry := log.NewLogEntry(update)
    c.Assert(log.Commit(logEntry), IsNil)
    c.Assert(logEntry.validateSignature(&log.memLog.PrivateKey.PublicKey), IsNil)
    c.Assert(logEntry.validateSignature(log.memLog.PublicKeys[logEntry.NodeId]), IsNil)
}

func (s *LogEntrySuite) TestSignAndValidateOfCopiedLog(c *C) {
    config := conf.LoadTest(s.dir, 0)
    log := newTestableLog(config)
    update := log.NewUpdate(Key(string(log.memLog.MyNodeId)+"/hello/2"), []byte("world"))
    logEntry := log.NewLogEntry(update)
    c.Assert(log.Commit(logEntry), IsNil)
    c.Assert(logEntry.validateSignature(&log.memLog.PrivateKey.PublicKey), IsNil)
    c.Assert(logEntry.validateSignature(log.memLog.PublicKeys[logEntry.NodeId]), IsNil)

    copyLogEntry := *logEntry
    copyUpdate := *update
    copyLogEntry.Message = &copyUpdate
    c.Assert(copyLogEntry.validateSignature(log.memLog.PublicKeys[copyLogEntry.NodeId]), IsNil)
}

func (s *LogEntrySuite) TestSignAndValidateRemovingValue(c *C) {
    config := conf.LoadTest(s.dir, 0)
    log := newTestableLog(config)
    update := log.NewUpdate(Key(string(log.memLog.MyNodeId)+"/hello/2"), []byte("world"))
    logEntry := log.NewLogEntry(update)
    c.Assert(log.Commit(logEntry), IsNil)
    c.Assert(update.value, IsNil)
    c.Assert(logEntry.validateSignature(&log.memLog.PrivateKey.PublicKey), IsNil)
    c.Assert(logEntry.validateSignature(log.memLog.PublicKeys[logEntry.NodeId]), IsNil)
}
