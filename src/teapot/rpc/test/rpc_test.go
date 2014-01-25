package test

import (
    . "launchpad.net/gocheck"
    "strconv"
    "teapot/conf"
    "teapot/log"
    "teapot/rpc/client"
    "teapot/rpc/server"
    "testing"
    "time"
)

func Test(t *testing.T) { TestingT(t) }

type RPCSuite struct {
    dir string
}

var _ = Suite(&RPCSuite{})

func (s *RPCSuite) SetUpSuite(c *C) {
    s.dir = c.MkDir()
}

func (s *RPCSuite) TestRPC(c *C) {
    config := conf.LoadTest(s.dir, 0)
    cl := client.NewClient(config)
    se := server.NewTeapotServer(config)
    server.AsyncStartServer(se)
    c.Assert(cl.Put(log.Key(config.MyNodeId+"/hello/0"), []byte("world")), IsNil)
    values, err := cl.Get(log.Key(config.MyNodeId + "/hello/0"))
    c.Assert(err, IsNil)
    c.Assert(len(values), Equals, 1)
    c.Assert(values[0], DeepEquals, []byte("world"))
    values, err = cl.Get(log.Key(config.MyNodeId + "/hello/nonexist"))
    c.Assert(err, ErrorMatches, "The key doesn't exist.*")
    // TODO chmod
    for i := 0; i < 5; i++ {
        c.Assert(cl.Put(log.Key(config.MyNodeId+"/hello/0"), []byte("world"+strconv.Itoa(i))), IsNil)
    }
    // test GC
    now := time.Now().Unix()
    c.Assert(cl.GC(), IsNil)
    // test GetVersions
    versions, err := cl.GetVersions()
    c.Assert(err, IsNil)
    c.Assert(len(versions), Equals, 1)
    num, _ := strconv.Atoi(versions[0])
    if now-int64(num) > 2 {
        c.Assert(true, Equals, false)
    }
}
