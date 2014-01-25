package log

import (
    "crypto/rand"
    "crypto/rsa"
    "encoding/base64"
    "fmt"
    . "launchpad.net/gocheck"
    "teapot/conf"
    "teapot/utility"
)

type UtilitySuite struct{}
type UtilityWithLogSuite struct {
    n    int
    dir  string
    logs []*Log
}

var _ = Suite(&UtilitySuite{})
var _ = Suite(&UtilityWithLogSuite{})

func (s *UtilityWithLogSuite) SetUpSuite(c *C) {
    s.dir = c.MkDir()
    s.n = 3
}

func (s *UtilityWithLogSuite) SetUpTest(c *C) {
    s.logs = nil
    configs := conf.LoadMultipleTest(s.dir, s.n)
    for _, config := range configs {
        //log := newTestableLog(config)
        log := NewLog(config)
        s.logs = append(s.logs, log)
    }
}

func (s *UtilitySuite) TestGetPartialVirtualNodeId(c *C) {
    actual := getPartialVirtualNodeId(NodeID("a"), EncodedHash("b"))
    c.Assert(actual, Equals, NodeID("a.b"))
}

func (s *UtilitySuite) TestGetNodeIdFromVirtualNodeId(c *C) {
    actual := getNodeIdFromVirtualNodeId(NodeID("ab"))
    c.Assert(actual, Equals, NodeID("ab"))
    actual = getNodeIdFromVirtualNodeId(NodeID("a.b"))
    c.Assert(actual, Equals, NodeID("a"))
    actual = getNodeIdFromVirtualNodeId(NodeID("a.b.c"))
    c.Assert(actual, Equals, NodeID("a"))
}

func (s *UtilitySuite) TestGetNodeIdFromVirtualNodeIdShouldPanic(c *C) {
    c.Assert(func() { getNodeIdFromVirtualNodeId(NodeID(".ab")) }, PanicMatches, "Invalid input:.*")
}

func (s *UtilitySuite) TestValidateKey(c *C) {
    c.Assert(ValidateKey("a/b/c"), IsNil)
    c.Assert(ValidateKey("/a/b/c"), NotNil)
    c.Assert(ValidateKey("a/b/c/"), NotNil)
    c.Assert(ValidateKey("a/b//c///"), NotNil)
    c.Assert(ValidateKey("a/b/c///a"), NotNil)
    c.Assert(ValidateKey("ab/b.c/c/a"), NotNil)
    c.Assert(ValidateKey("ab/bc/c.a"), IsNil)
    c.Assert(ValidateKey("ab/bc/c.a/a"), IsNil)
    c.Assert(ValidateKey("ab/bc/c.a/adskalfjdlaskjflkdsajlfjasdlkjflasjdkfljadslkfjalksdjfldskjflksadjlkfjasdkljflkdsajflkasjdlfkasjklfjasdklfjkldsajfkldsjafkljdsalkjflkadsjfkldasjlfkjdsalkfjsadklfjaskdld"), NotNil)
}

func (s *UtilitySuite) TestSplitKey(c *C) {
    nodeId, dir, p, err := splitKey(Key("0/default/hello/world"))
    c.Assert(err, IsNil)
    c.Assert(nodeId, Equals, NodeID("0"))
    c.Assert(dir, Equals, Dir("default"))
    c.Assert(p, Equals, path("hello/world"))
    // code to handle these two cases
    nodeId, dir, p, err = splitKey(Key("0/default/hello/world/"))
    c.Assert(err, IsNil)
    c.Assert(nodeId, Equals, NodeID("0"))
    c.Assert(dir, Equals, Dir("default"))
    c.Assert(p, Equals, path("hello/world"))

    nodeId, dir, p, err = splitKey(Key("0/default//hello/world/"))
    c.Assert(err, IsNil)
    c.Assert(nodeId, Equals, NodeID("0"))
    c.Assert(dir, Equals, Dir("default"))
    c.Assert(p, Equals, path("hello/world"))
}

func (s *UtilitySuite) TestSplitKeyShouldPanic(c *C) {
    _, _, _, err := splitKey(Key("0/default"))
    c.Assert(err, ErrorMatches, "The key should be formed as nodeId/directory/path.*")
}

func (s *UtilitySuite) TestExtractKey(c *C) {
    privKey, err := rsa.GenerateKey(rand.Reader, 1024)
    c.Assert(err, IsNil)
    roq := utility.GetHashOfBytesAndEncode([]byte("hello"))
    cipher, err := rsa.EncryptPKCS1v15(rand.Reader, &privKey.PublicKey, []byte("hello"))
    c.Assert(err, IsNil)
    encodedKey := encodedEncryptedSecretKey(base64.URLEncoding.EncodeToString(cipher))
    key, err := extractKey(encodedKey, privKey, roq)
    c.Assert(err, IsNil)
    c.Assert(key, DeepEquals, []byte("hello"))
}

// TODO Test build full DVV of an update.
func (s *UtilityWithLogSuite) TestBuildFullDVV(c *C) {
    // 0 grants 1 write access first and then revoke it.
    // 1 ignores the second revocation, all updates from A should be able to commit.
    // node index to perform commit
    commit := []int{0, 1, 0, 1, 0, 1, 0, 1, 0, 1}
    // index of log to commit, -1 means new update, -2 means changemode
    logIndex := []int{-1, 0, -1, -1, 2, -1, 3, -1, 4, 1}
    // -1 means no reader/writer, the index should be less than s.n
    // should only be non--1 when logIndex is -2.
    c.Assert(len(commit), Equals, len(logIndex))
    logEntries := make([]*LogEntry, 0)
    for i := 0; i < len(commit); i++ {
        log := s.logs[commit[i]]
        var logEntry *LogEntry
        if logIndex[i] == -1 {
            // issue new updates
            update := log.NewUpdate(Key(fmt.Sprintf("%v/hello/%v", log.memLog.MyNodeId, i)), []byte(fmt.Sprintf("world%v", i)))
            logEntry = log.NewLogEntry(update)
            logEntries = append(logEntries, logEntry)
        } else {
            logEntry = logEntries[logIndex[i]]
        }
        c.Assert(log.Commit(logEntry), IsNil)
    }
    c.Assert(len(logEntries), Equals, 5)
    fullDVV := make(versionVector)
    fullDVV = buildFullDvv(s.logs[1], logEntries[0].DVV, fullDVV)
    c.Assert(len(fullDVV), Equals, 0)
    fullDVV = make(versionVector)
    fullDVV = buildFullDvv(s.logs[1], logEntries[2].DVV, fullDVV)
    c.Assert(fullDVV[s.logs[0].memLog.MyNodeId].AcceptStamp, Equals, Timestamp(1))
    fullDVV = make(versionVector)
    fullDVV = buildFullDvv(s.logs[0], logEntries[2].DVV, fullDVV)
    c.Assert(fullDVV[s.logs[0].memLog.MyNodeId].AcceptStamp, Equals, Timestamp(1))

    fullDVV = make(versionVector)
    fullDVV = buildFullDvv(s.logs[1], logEntries[3].DVV, fullDVV)
    c.Assert(fullDVV[s.logs[0].memLog.MyNodeId].AcceptStamp, Equals, Timestamp(1))
    fullDVV = make(versionVector)
    fullDVV = buildFullDvv(s.logs[0], logEntries[3].DVV, fullDVV)
    c.Assert(fullDVV[s.logs[0].memLog.MyNodeId].AcceptStamp, Equals, Timestamp(1))

    fullDVV = make(versionVector)
    fullDVV = buildFullDvv(s.logs[1], logEntries[4].DVV, fullDVV)
    c.Assert(fullDVV[s.logs[0].memLog.MyNodeId].AcceptStamp, Equals, Timestamp(1))
    fullDVV = make(versionVector)
    fullDVV = buildFullDvv(s.logs[0], logEntries[4].DVV, fullDVV)
    c.Assert(fullDVV[s.logs[0].memLog.MyNodeId].AcceptStamp, Equals, Timestamp(1))
}
