package teapot

import (
    . "launchpad.net/gocheck"
    "strconv"
    "teapot/conf"
    "teapot/log"
    "teapot/utility"
    "testing"
    "time"
)

// Hook up gocheck into the gotest runner.
func Test(t *testing.T) { TestingT(t) }

type TeapotSuite struct {
    n   int
    dir string
}

var _ = Suite(&TeapotSuite{})

func (s *TeapotSuite) SetUpSuite(c *C) {
    s.n = 3
    s.dir = c.MkDir()
    defaultLogExInterval = 1
    logExInterval = 1
    logExUpperBound = 4
}

func (s *TeapotSuite) TestTeapot(c *C) {
    config := conf.LoadTest(s.dir, 0)
    teapot := NewTeapot(config)
    err := teapot.Put(log.Key(config.MyNodeId+"/default/hello"), []byte("world"))
    c.Assert(err, IsNil)
    data, err := teapot.Get(log.Key(config.MyNodeId + "/default/hello"))
    c.Assert(err, IsNil)
    c.Assert(len(data), Equals, 1)
    c.Assert(string(data[0]), Equals, "world")
    data, err = teapot.Get(log.Key(config.MyNodeId + "/default/nonexist"))
    c.Assert(err, ErrorMatches, "The key doesn't exist.*")
}

func (s *TeapotSuite) TestComplexTeapot(c *C) {
    config := conf.LoadTest(s.dir, 0)
    teapot := NewTeapot(config)
    for i := 0; i < 5; i++ {
        err := teapot.Put(log.Key(config.MyNodeId+"/default/hello"), []byte("world"+strconv.Itoa(i)))
        c.Assert(err, IsNil)
        data, err := teapot.Get(log.Key(config.MyNodeId + "/default/hello"))
        c.Assert(err, IsNil)
        c.Assert(len(data), Equals, 1)
        c.Assert(string(data[0]), Equals, "world"+strconv.Itoa(i))
    }
    c.Assert(teapot.log.GC(), IsNil)
    for i := 5; i < 10; i++ {
        err := teapot.Put(log.Key(config.MyNodeId+"/default/hello"), []byte("world"+strconv.Itoa(i)))
        c.Assert(err, IsNil)
        data, err := teapot.Get(log.Key(config.MyNodeId + "/default/hello"))
        c.Assert(err, IsNil)
        c.Assert(len(data), Equals, 1)
        c.Assert(string(data[0]), Equals, "world"+strconv.Itoa(i))
    }
    data, err := teapot.GetVersion(log.Key(config.MyNodeId+"/default/hello"), 0)
    c.Assert(err, IsNil)
    c.Assert(string(data[0]), Equals, "world"+strconv.Itoa(4))
}

func (s *TeapotSuite) TestMultipleClientTeapot(c *C) {
    configs := conf.LoadMultipleTest(s.dir, s.n)
    teapots := make([]*Teapot, 0)
    for _, config := range configs {
        teapot := NewTeapot(config)
        c.Assert(teapot, NotNil)
        teapots = append(teapots, teapot)
    }
    for i := 0; i < s.n; i++ {
        teapot := teapots[i]
        config := configs[i]
        err := teapot.Put(log.Key(config.MyNodeId+"/default/hello"), []byte("world"+strconv.Itoa(i)))
        c.Assert(err, IsNil)
    }
    //wait for log ex
    time.Sleep(10 * time.Second)
    for i := 0; i < s.n; i++ {
        for j := 0; j < s.n; j++ {
            data, err := teapots[i].Get(log.Key(configs[j].MyNodeId + "/default/hello"))
            if i == j {
                c.Assert(err, IsNil)
                c.Assert(len(data), Equals, 1)
                c.Assert(string(data[0]), Equals, "world"+strconv.Itoa(i))
            } else {
                c.Assert(data, IsNil)
                c.Assert(err, ErrorMatches, "Read Access Denied.*")
            }
        }
    }
    for i := 0; i < s.n; i++ {
        teapot := teapots[i]
        err := teapot.ChangeMode("default", utility.KeyFromPassphrase("password"+strconv.Itoa(i)), []log.NodeID{}, []log.NodeID{log.NodeID(configs[0].MyNodeId), log.NodeID(configs[1].MyNodeId), log.NodeID(configs[2].MyNodeId)})
        c.Assert(err, IsNil)
    }
    for i := 0; i < s.n; i++ {
        teapot := teapots[i]
        config := configs[i]
        err := teapot.Put(log.Key(config.MyNodeId+"/default/hello"), []byte("world"+strconv.Itoa(s.n+i)))
        c.Assert(err, IsNil)
    }
    for i := 0; i < s.n; i++ {
        teapot := teapots[i]
        config := configs[i]
        err := teapot.Put(log.Key(config.MyNodeId+"/default/hello"), []byte("world"+strconv.Itoa(s.n*2+i)))
        c.Assert(err, IsNil)
    }
    // wait for log ex.
    time.Sleep(20 * time.Second)
    // everyone should be able to read each other's update.
    for i := 0; i < s.n; i++ {
        for j := 0; j < s.n; j++ {
            data, err := teapots[i].Get(log.Key(configs[j].MyNodeId + "/default/hello"))
            c.Assert(err, IsNil)
            c.Assert(len(data), Equals, 1)
            c.Assert(string(data[0]), Equals, "world"+strconv.Itoa(s.n*2+j))
        }
    }
    // GC
    c.Assert(teapots[0].GC(), IsNil)

    time.Sleep(20 * time.Second)
    for i := 0; i < s.n; i++ {
        teapot := teapots[i]
        config := configs[i]
        err := teapot.Put(log.Key(config.MyNodeId+"/default/hello"), []byte("world"+strconv.Itoa(s.n*3+i)))
        c.Assert(err, IsNil)
    }
    time.Sleep(10 * time.Second)
    for i := 0; i < s.n; i++ {
        for j := 0; j < s.n; j++ {
            data, err := teapots[i].Get(log.Key(configs[j].MyNodeId + "/default/hello"))
            c.Assert(err, IsNil)
            c.Assert(len(data), Equals, 1)
            c.Assert(string(data[0]), Equals, "world"+strconv.Itoa(s.n*3+j))
        }
    }
    for i := 0; i < s.n; i++ {
        for j := 0; j < s.n; j++ {
            data, err := teapots[i].GetVersion(log.Key(configs[j].MyNodeId+"/default/hello"), 0)
            c.Assert(err, IsNil)
            c.Assert(len(data), Equals, 1)
            c.Assert(string(data[0]), Equals, "world"+strconv.Itoa(2*s.n+j))
        }
    }
}
