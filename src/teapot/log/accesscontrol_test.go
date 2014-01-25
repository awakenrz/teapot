package log

import (
    "fmt"
    . "launchpad.net/gocheck"
    "math/rand"
    //    "os"
    "strconv"
    "teapot/conf"
    "teapot/utility"
)

type ACSuite struct {
    n    int
    dir  string
    logs []*Log
}

var _ = Suite(&ACSuite{})

func (s *ACSuite) SetUpSuite(c *C) {
    s.dir = c.MkDir()
    s.n = 3
}

func (s *ACSuite) TearDownSuite(c *C) {
}

func (s *ACSuite) SetUpTest(c *C) {
    s.logs = nil
    configs := conf.LoadMultipleTest(s.dir, s.n)
    for _, config := range configs {
        //log := newTestableLog(config)
        log := NewLog(config)
        s.logs = append(s.logs, log)
    }
}

func (s *ACSuite) TestGetKey(c *C) {
    log := s.logs[0]
    for i := 0; i < 5; i++ {
        update := log.NewUpdate(Key(string(log.memLog.MyNodeId)+"/dir/"+strconv.Itoa(i)), []byte(strconv.Itoa(i)))
        logEntry := log.NewLogEntry(update)
        c.Assert(log.Commit(logEntry), IsNil)
        // No mode is issued yet. So it should be nil
        //c.Assert(log.getMode(logEntry), IsNil)
        c.Assert(log.getReadKey(update.Key, logEntry.DVV), DeepEquals, log.memLog.DefaultKey)
    }
    logEntry := log.getIthEntryOfNode(log.memLog.MyNodeId, 4)
    c.Assert(logEntry, NotNil)
    update, ok := logEntry.Message.(*Update)
    c.Assert(ok, Equals, true)
    c.Assert(log.getReadKey(update.Key, logEntry.DVV), DeepEquals, log.memLog.DefaultKey)

    chmod := log.NewChangeMode("dir", utility.KeyFromPassphrase("password"), []NodeID{}, []NodeID{log.memLog.MyNodeId})
    // verifies that owner is automatically added into writer list.
    c.Assert(chmod.Writers[log.memLog.MyNodeId], Not(Equals), encodedEncryptedSecretKey(""))
    c.Assert(chmod.Writers["nobody"], Equals, encodedEncryptedSecretKey(""))

    logEntry = log.NewLogEntry(chmod)
    c.Assert(log.Commit(logEntry), IsNil)

    for i := 0; i < 5; i++ {
        update := log.NewUpdate(Key(string(log.memLog.MyNodeId)+"/dir1/"+strconv.Itoa(i)), []byte(strconv.Itoa(i)))
        logEntry := log.NewLogEntry(update)
        c.Assert(log.Commit(logEntry), IsNil)
        // No mode to this directory, so it should still be nil
        c.Assert(log.getReadKey(update.Key, logEntry.DVV), DeepEquals, log.memLog.DefaultKey)
    }

    chmod = log.NewChangeMode("dir1", utility.KeyFromPassphrase("password1"), []NodeID{}, []NodeID{log.memLog.MyNodeId})
    logEntry = log.NewLogEntry(chmod)
    c.Assert(log.Commit(logEntry), IsNil)

    for i := 0; i < 5; i++ {
        update := log.NewUpdate(Key(string(log.memLog.MyNodeId)+"/dir/"+strconv.Itoa(i)), []byte(strconv.Itoa(i)))
        logEntry := log.NewLogEntry(update)
        c.Assert(log.Commit(logEntry), IsNil)
        c.Assert(log.getReadKey(update.Key, logEntry.DVV), DeepEquals, SecretKey(utility.KeyFromPassphrase("password")))
    }
    logEntry = log.getIthEntryOfNode(log.memLog.MyNodeId, 4)
    c.Assert(logEntry, NotNil)
    update, ok = logEntry.Message.(*Update)
    c.Assert(ok, Equals, true)
    c.Assert(log.getReadKey(update.Key, logEntry.DVV), DeepEquals, log.memLog.DefaultKey)
}

func (s *ACSuite) TestGetKeyBug(c *C) {

}

func (s *ACSuite) newChangeMode(log *Log, readerIndex, writerIndex int) *ChangeMode {
    readers := []NodeID{}
    if readerIndex != -1 {
        readers = append(readers, s.logs[readerIndex].memLog.MyNodeId)
    }
    writers := []NodeID{}
    if writerIndex != -1 {
        writers = append(writers, s.logs[writerIndex].memLog.MyNodeId)
    }
    key := SecretKey(utility.KeyFromPassphrase(fmt.Sprintf("%v", rand.Int()%5)))
    chmod := log.NewChangeMode(Dir("hello"), key, readers, writers)
    return chmod
}

func (s *ACSuite) TestChangeMode(c *C) {
    // node index to perform commit
    commit := []int{0, 1, 0, 1, 0, 1}
    // index of log to commit, -1 means new update, -2 means changemode
    logIndex := []int{-1, 0, -2, 1, -1, 2}
    // -1 means no reader/writer, the index should be less than s.n
    readerIndex := []int{-1, -1, 1, -1, -1, -1}
    writerIndex := []int{-1, -1, -1, -1, -1, -1}
    logEntries := make([]*LogEntry, 0)
    for i := 0; i < len(commit); i++ {
        log := s.logs[commit[i]]
        var logEntry *LogEntry
        if logIndex[i] == -1 {
            // issue new updates
            update := log.NewUpdate(Key(fmt.Sprintf("%v/hello/%v", log.memLog.MyNodeId, i)), []byte(fmt.Sprintf("world%v", i)))
            logEntry = log.NewLogEntry(update)
            logEntries = append(logEntries, logEntry)
        } else if logIndex[i] == -2 {
            // issue change mode
            chmod := s.newChangeMode(log, readerIndex[i], writerIndex[i])
            logEntry = log.NewLogEntry(chmod)
            logEntries = append(logEntries, logEntry)
        } else {
            logEntry = logEntries[logIndex[i]]
        }
        c.Assert(log.Commit(logEntry), IsNil)
    }
    // now, 1 get 0/hello/0 should fail
    updates, err := s.logs[1].GetCheckpoint(Key(fmt.Sprintf("%v/hello/%v", s.logs[0].memLog.MyNodeId, 0)))
    c.Assert(err, IsNil)
    c.Assert(len(updates), Equals, 1)
    _, err = s.logs[1].GetDecryptValue(updates[0].Message.(*Update), updates[0].DVV)
    c.Assert(err, ErrorMatches, "Read Access Denied.*")
    // now, 1 get 0/hello/4 should succeed
    updates, err = s.logs[1].GetCheckpoint(Key(fmt.Sprintf("%v/hello/%v", s.logs[0].memLog.MyNodeId, 4)))
    c.Assert(err, IsNil)
    c.Assert(len(updates), Equals, 1)
    value, err := s.logs[1].GetDecryptValue(updates[0].Message.(*Update), updates[0].DVV)
    c.Assert(err, Not(ErrorMatches), "Read Access Denied.*")
    value, err = s.logs[0].GetDecryptValue(updates[0].Message.(*Update), updates[0].DVV)
    c.Assert(err, IsNil)
    c.Assert(value, DeepEquals, []byte("world4"))
}

func (s *ACSuite) TestRevoke(c *C) {
    // 0 grants 1 write access first and then revoke it.
    // 1 ignores the second revocation, all updates from A should be able to commit.
    // node index to perform commit
    commit := []int{0, 1, 0}
    // index of log to commit, -1 means new update, -2 means changemode
    logIndex := []int{-2, 0, -2}
    // -1 means no reader/writer, the index should be less than s.n
    // should only be non--1 when logIndex is -2.
    readerIndex := []int{-1, -1, 1}
    writerIndex := []int{1, -1, -1}
    c.Assert(len(commit), Equals, len(logIndex))
    c.Assert(len(commit), Equals, len(readerIndex))
    c.Assert(len(commit), Equals, len(writerIndex))
    logEntries := make([]*LogEntry, 0)
    for i := 0; i < len(commit); i++ {
        log := s.logs[commit[i]]
        var logEntry *LogEntry
        if logIndex[i] == -1 {
            // issue new updates
            update := log.NewUpdate(Key(fmt.Sprintf("%v/hello/%v", log.memLog.MyNodeId, i)), []byte(fmt.Sprintf("world%v", i)))
            logEntry = log.NewLogEntry(update)
            logEntries = append(logEntries, logEntry)
        } else if logIndex[i] == -2 {
            // issue change mode
            chmod := s.newChangeMode(log, readerIndex[i], writerIndex[i])
            logEntry = log.NewLogEntry(chmod)
            logEntries = append(logEntries, logEntry)
        } else {
            logEntry = logEntries[logIndex[i]]
        }
        c.Assert(log.Commit(logEntry), IsNil)
    }
    c.Assert(len(s.logs[0].memLog.WriteKeyInfo[s.logs[0].memLog.MyNodeId][Dir("hello")]), Equals, 2)
    log := s.logs[1]
    update := log.NewUpdate(Key(fmt.Sprintf("%v/hello/%v", s.logs[0].memLog.MyNodeId, 4)), []byte(fmt.Sprintf("world%v", 4)))
    logEntry := log.NewLogEntry(update)
    c.Assert(log.Commit(logEntry), IsNil)
    c.Assert(s.logs[0].Commit(logEntry), IsNil)
    update = log.NewUpdate(Key(fmt.Sprintf("%v/hello/%v", s.logs[0].memLog.MyNodeId, 5)), []byte(fmt.Sprintf("world%v", 5)))
    logEntry = log.NewLogEntry(update)
    c.Assert(log.Commit(logEntry), IsNil)
    c.Assert(s.logs[0].Commit(logEntry), IsNil)
    update = log.NewUpdate(Key(fmt.Sprintf("%v/hello/%v", s.logs[0].memLog.MyNodeId, 6)), []byte(fmt.Sprintf("world%v", 6)))
    logEntry = log.NewLogEntry(update)
    c.Assert(log.Commit(logEntry), IsNil)
    c.Assert(s.logs[0].Commit(logEntry), IsNil)
    c.Assert(log.Commit(logEntries[1]), IsNil)
    update = log.NewUpdate(Key(fmt.Sprintf("%v/hello/%v", s.logs[0].memLog.MyNodeId, 7)), []byte(fmt.Sprintf("world%v", 7)))
    logEntry = log.NewLogEntry(update)
    c.Assert(log.Commit(logEntry), ErrorMatches, "Write Access Denied.*")
}

// TODO
func (s *ACSuite) TestComplexChangeMode(c *C) {
    // node index to perform commit
    commit := []int{0, 1, 0, 1, 0, 1, 2, 2, 2, 1, 1, 0, 2, 0, 2}
    // index of log to commit, -1 means new update, -2 means changemode
    logIndex := []int{-1, 0, -2, 1, -1, 2, 0, 1, 2, -2, -1, 3, 3, 4, 4}
    // -1 means no reader/writer, the index should be less than s.n
    // should only be non--1 when logIndex is -2.
    readerIndex := []int{-1, -1, 1, -1, -1, -1, -1, -1, -1, 0, -1, -1, -1, -1, -1}
    writerIndex := []int{-1, -1, -1, -1, -1, -1, -1, -1, -1, 2, -1, -1, -1, -1, -1}
    c.Assert(len(commit), Equals, len(logIndex))
    c.Assert(len(commit), Equals, len(readerIndex))
    c.Assert(len(commit), Equals, len(writerIndex))
    logEntries := make([]*LogEntry, 0)
    for i := 0; i < len(commit); i++ {
        log := s.logs[commit[i]]
        var logEntry *LogEntry
        if logIndex[i] == -1 {
            // issue new updates
            update := log.NewUpdate(Key(fmt.Sprintf("%v/hello/%v", log.memLog.MyNodeId, i)), []byte(fmt.Sprintf("world%v", i)))
            logEntry = log.NewLogEntry(update)
            logEntries = append(logEntries, logEntry)
        } else if logIndex[i] == -2 {
            // issue change mode
            chmod := s.newChangeMode(log, readerIndex[i], writerIndex[i])
            logEntry = log.NewLogEntry(chmod)
            logEntries = append(logEntries, logEntry)
        } else {
            logEntry = logEntries[logIndex[i]]
        }
        c.Assert(log.Commit(logEntry), IsNil)
    }
    // now, 1 get 0/hello/0 should still fail because it is not covered by the change mode.
    updates, err := s.logs[1].GetCheckpoint(Key(fmt.Sprintf("%v/hello/%v", s.logs[0].memLog.MyNodeId, 0)))
    c.Assert(err, IsNil)
    c.Assert(len(updates), Equals, 1)
    _, err = s.logs[1].GetDecryptValue(updates[0].Message.(*Update), updates[0].DVV)
    c.Assert(err, ErrorMatches, "Read Access Denied.*")

    // now, 1 get 0/hello/4 should succeed
    updates, err = s.logs[1].GetCheckpoint(Key(fmt.Sprintf("%v/hello/%v", s.logs[0].memLog.MyNodeId, 4)))
    c.Assert(err, IsNil)
    c.Assert(len(updates), Equals, 1)
    value, err := s.logs[1].GetDecryptValue(updates[0].Message.(*Update), updates[0].DVV)
    c.Assert(err, Not(ErrorMatches), "Read Access Denied.*")
    value, err = s.logs[0].GetDecryptValue(updates[0].Message.(*Update), updates[0].DVV)
    c.Assert(err, IsNil)
    c.Assert(value, DeepEquals, []byte("world4"))

    // now, 2 get 0/hello/4 should fail
    updates, err = s.logs[2].GetCheckpoint(Key(fmt.Sprintf("%v/hello/%v", s.logs[0].memLog.MyNodeId, 4)))
    c.Assert(err, IsNil)
    c.Assert(len(updates), Equals, 1)
    _, err = s.logs[2].GetDecryptValue(updates[0].Message.(*Update), updates[0].DVV)
    c.Assert(err, ErrorMatches, "Read Access Denied.*")

    // now, 2 get 1/hello/10 should succeed
    updates, err = s.logs[2].GetCheckpoint(Key(fmt.Sprintf("%v/hello/%v", s.logs[1].memLog.MyNodeId, 10)))
    c.Assert(err, IsNil)
    c.Assert(len(updates), Equals, 1)
    value, err = s.logs[1].GetDecryptValue(updates[0].Message.(*Update), updates[0].DVV)
    c.Assert(err, IsNil)
    c.Assert(value, DeepEquals, []byte("world10"))
    // now, 0 get 1/hello/10 should succeed
    updates, err = s.logs[0].GetCheckpoint(Key(fmt.Sprintf("%v/hello/%v", s.logs[1].memLog.MyNodeId, 10)))
    c.Assert(err, IsNil)
    c.Assert(len(updates), Equals, 1)
    value, err = s.logs[1].GetDecryptValue(updates[0].Message.(*Update), updates[0].DVV)
    c.Assert(err, IsNil)
    c.Assert(value, DeepEquals, []byte("world10"))
    // now 2 should be able to write to 1/hello/*
    update := s.logs[2].NewUpdate(Key(fmt.Sprintf("%v/hello/%v", s.logs[1].memLog.MyNodeId, 32767)), []byte(fmt.Sprintf("world%v", 32767)))
    logEntry := s.logs[2].NewLogEntry(update)
    c.Assert(s.logs[2].Commit(logEntry), IsNil)
    c.Assert(s.logs[0].Commit(logEntry), IsNil)
    c.Assert(s.logs[1].Commit(logEntry), IsNil)
}
