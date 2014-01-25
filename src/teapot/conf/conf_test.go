package conf

import (
    . "launchpad.net/gocheck"
    "teapot/utility"
    "testing"
)

// Hook up gocheck into the gotest runner.
func Test(t *testing.T) { TestingT(t) }

type S struct {
    dir string
}

var _ = Suite(&S{})

func (s *S) SetUpSuite(c *C) {
    s.dir = c.MkDir()
}

func (s *S) TestConfig(c *C) {
    // Generate the configration and then test
    encodedPriKey, encodedPubKey := utility.GenerateKeyPairAndEncode()
    nodeInfo := GenerateNodeInfo("test_node1", "127.0.0.1:12346", encodedPubKey)
    configuration := GenerateConfig(nodeInfo, "abcd", "test_aws_access_key", "test_aws_secret_key", encodedPriKey)
    WriteConfigFile(configuration, s.dir+"/teapot.config")
    config, err := LoadFromFile(s.dir + "/teapot.config")
    c.Assert(err, IsNil)
    c.Assert(config.LogPath, Equals, "log.txt")
    c.Assert(config.JournalPath, Equals, "journal.txt")
    c.Assert(config.SnapshotDir, Equals, "snapshot/")
    c.Assert(config.ValueDir, Equals, "values/")
    c.Assert(config.MyNodeId, Equals, "test_node1")
    c.Assert(config.MyBucketName, Not(Equals), "")
    c.Assert(config.IPPort, Equals, "127.0.0.1:12346")
    c.Assert(config.DaemonPort, Not(Equals), 0)

    c.Assert(config.AESKey, DeepEquals, utility.KeyFromPassphrase("abcd"))
}
