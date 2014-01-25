package main

import (
    . "launchpad.net/gocheck"
    "testing"
)

// Hook up gocheck into the gotest runner.
func Test(t *testing.T) { TestingT(t) }

type MySuite struct{}

var _ = Suite(&MySuite{})

func (s *MySuite) TestNodeIdValidation(c *C) {
    c.Check(checkNodeId("Abz09"), Equals, true)
    c.Check(checkNodeId("Abz-0"), Equals, false)
    c.Check(checkNodeId("Abz_0"), Equals, false)
    c.Check(checkNodeId("Abz$0"), Equals, false)
}

func (s *MySuite) TestPassphraseValidation(c *C) {
    c.Check(checkPassphrase("Abz09"), Equals, true)
    c.Check(checkPassphrase("Abz-0"), Equals, true)
    c.Check(checkPassphrase("Abz_0"), Equals, true)
    c.Check(checkPassphrase("Abz$0"), Equals, true)
}

func (s *MySuite) TestIpAddressValidation(c *C) {
    c.Check(checkIpAddress("192.168.0.1:12345"), Equals, true)
    c.Check(checkIpAddress("192.168.0.1"), Equals, false)
    c.Check(checkIpAddress("192.168.0.256:12345"), Equals, false)
    c.Check(checkIpAddress("192.168.0.1:123456"), Equals, false)
    c.Check(checkIpAddress("192.168.1:12345"), Equals, false)
}
