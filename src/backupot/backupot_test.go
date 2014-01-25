package main

import (
    "io/ioutil"
    . "launchpad.net/gocheck"
    "os"
    "teapot/conf"
    "teapot/rpc/server"
    "testing"
)

func Test(t *testing.T) { TestingT(t) }

type S struct {
    dir      string
    filename string
    config   *conf.Config
    server   *server.TeapotServer
}

type RealSuite struct {
    dir    string
    config *conf.Config
    server *server.TeapotServer
}

var _ = Suite(&S{})
var _ = Suite(&RealSuite{})

func (s *S) SetUpSuite(c *C) {
}

func (s *S) TearDownSuite(c *C) {
}

func (s *RealSuite) SetUpSuite(c *C) {
    defer func() {
        if r := recover(); r != nil {
            c.Skip("Cannot load config.")
        }
    }()
    s.dir = c.MkDir()
    s.config, _ = conf.LoadFromFile("teapot.config")
    s.server = server.NewTeapotServer(s.config)
    server.AsyncStartServer(s.server)
}

func (s *RealSuite) TearDownSuite(c *C) {
    os.RemoveAll("log.txt")
    os.RemoveAll("log.txt")
    os.RemoveAll("values")
    os.RemoveAll("snapshot")
}

func (s *S) SetUpTest(c *C) {
    s.dir = c.MkDir()
    s.filename = s.dir + "/output.txt"
    s.config = conf.LoadTest(s.dir, 0)
    s.server = server.NewTeapotServer(s.config)
    server.AsyncStartServer(s.server)
}

func (s *S) TearDownTest(c *C) {
    //    s.server.StopServer()
}

func (s *S) TestBasicAssumption(c *C) {
    dir, name := splitPath("/abc/def/")
    c.Assert(dir, Equals, localPath("/abc/def/"))
    c.Assert(name, Equals, "")
    dir, name = splitPath("abc/def")
    c.Assert(dir, Equals, localPath("abc/"))
    c.Assert(name, Equals, "def")
}

func (s *S) TestBackuPOT(c *C) {
    b := NewBackuPOT(s.config)
    remotepath := remotePath(s.config.MyNodeId + "/dir/foo")
    localpath := localPath(s.dir + "/output.txt")
    // write something to output.txt
    c.Assert(ioutil.WriteFile(string(localpath), []byte("bar,hello_world"), 0700), IsNil)
    c.Assert(b.backup(localpath, remotepath), IsNil)
    c.Assert(b.restore(remotepath, localPath(s.dir+"/output.txt.1"), -1), IsNil)
    output, err := ioutil.ReadFile(s.dir + "/output.txt.1")
    c.Assert(err, IsNil)
    c.Assert(output, DeepEquals, []byte("bar,hello_world"))
    // write something else to output.txt
    c.Assert(ioutil.WriteFile(string(localpath), []byte("bar,again,hello_world"), 0700), IsNil)
    c.Assert(b.backup(localpath, remotepath), IsNil)
    c.Assert(b.restore(remotepath, localPath(s.dir+"/output.txt.2"), -1), IsNil)
    output, err = ioutil.ReadFile(s.dir + "/output.txt.2")
    c.Assert(err, IsNil)
    c.Assert(output, DeepEquals, []byte("bar,again,hello_world"))
}

func (s *S) TestBackuPOTRestoreToDirNotExist(c *C) {
    b := NewBackuPOT(s.config)
    remotepath := remotePath(s.config.MyNodeId + "/dir/foo")
    localpath := localPath(s.dir + "/output.txt")
    // write something to output.txt
    c.Assert(ioutil.WriteFile(string(localpath), []byte("bar,hello_world"), 0700), IsNil)
    c.Assert(b.backup(localpath, remotepath), IsNil)
    c.Assert(b.restore(remotepath, localPath(s.dir+"/foo/bar/output.txt"), -1), IsNil)
    output, err := ioutil.ReadFile(s.dir + "/foo/bar/output.txt")
    c.Assert(err, IsNil)
    c.Assert(output, DeepEquals, []byte("bar,hello_world"))
}

func (s *S) TestBackuPOTBackupFromDir(c *C) {
    b := NewBackuPOT(s.config)
    remotepath := remotePath(s.config.MyNodeId + "/dir/foo")
    localpath := localPath(s.dir + "/foo/output.txt")
    // write something to output.txt
    os.MkdirAll(s.dir+"/foo", 0700)
    c.Assert(ioutil.WriteFile(string(localpath), []byte("bar,hello_world"), 0700), IsNil)
    c.Assert(b.backup(localpath, remotepath), IsNil)
    c.Assert(b.restore(remotepath, localpath, -1), IsNil)
    output, err := ioutil.ReadFile(string(localpath))
    c.Assert(err, IsNil)
    c.Assert(output, DeepEquals, []byte("bar,hello_world"))
}

func (s *S) TestBackuPOTBackupDir(c *C) {
    b := NewBackuPOT(s.config)
    remotepath := remotePath(s.config.MyNodeId + "/dir/foo")
    // write something to output.txt
    os.MkdirAll(s.dir+"/foo/bar", 0700)
    c.Assert(ioutil.WriteFile(s.dir+"/foo/bar/output.txt", []byte("bar,hello_world"), 0700), IsNil)
    c.Assert(b.backup(localPath(s.dir+"/foo/bar"), remotepath), IsNil)
    c.Assert(b.restore(remotepath, localPath(s.dir+"/foo/boo"), -1), IsNil)
    output, err := ioutil.ReadFile(s.dir + "/foo/boo/output.txt")
    c.Assert(err, IsNil)
    c.Assert(output, DeepEquals, []byte("bar,hello_world"))
}

func (s *S) TestBackuPOTBackupDir1(c *C) {
    b := NewBackuPOT(s.config)
    remotepath := remotePath(s.config.MyNodeId + "/dir/foo")
    // write something to output.txt
    os.MkdirAll(s.dir+"/foo/bar", 0700)
    c.Assert(ioutil.WriteFile(s.dir+"/foo/bar/output.txt", []byte("bar,hello_world"), 0700), IsNil)
    c.Assert(b.backup(localPath(s.dir+"/foo/bar/"), remotepath), IsNil)
    c.Assert(b.restore(remotepath, localPath(s.dir+"/foo/boo"), -1), IsNil)
    output, err := ioutil.ReadFile(s.dir + "/foo/boo/output.txt")
    c.Assert(err, IsNil)
    c.Assert(output, DeepEquals, []byte("bar,hello_world"))
}

func (s *S) TestBackuPOTRestoreToDir(c *C) {
    b := NewBackuPOT(s.config)
    remotepath := remotePath(s.config.MyNodeId + "/dir/foo")
    // write something to output.txt
    os.MkdirAll(s.dir+"/foo/bar", 0700)
    c.Assert(ioutil.WriteFile(s.dir+"/foo/bar/output.txt", []byte("bar,hello_world"), 0700), IsNil)
    c.Assert(b.backup(localPath(s.dir+"/foo/bar"), remotepath), IsNil)
    c.Assert(b.restore(remotepath, localPath(s.dir+"/foo/boo/"), -1), IsNil)
    output, err := ioutil.ReadFile(s.dir + "/foo/boo/bar/output.txt")
    c.Assert(err, IsNil)
    c.Assert(output, DeepEquals, []byte("bar,hello_world"))
    c.Assert(b.backup(localPath(s.dir+"/foo/bar/output.txt"), remotepath), IsNil)
    c.Assert(b.restore(remotepath, localPath(s.dir+"/foo/boo/"), -1), IsNil)
    output, err = ioutil.ReadFile(s.dir + "/foo/boo/output.txt")
    c.Assert(err, IsNil)
    c.Assert(output, DeepEquals, []byte("bar,hello_world"))
}

func (s *RealSuite) TestBackup(c *C) {
    b := NewBackuPOT(s.config)
    remotepath := remotePath(s.config.MyNodeId + "/dir/foo")
    // write something to output.txt
    c.Assert(ioutil.WriteFile(s.dir+"/output.txt", []byte("bar,hello_world"), 0700), IsNil)
    c.Assert(b.backup(localPath(s.dir+"/output.txt"), remotepath), IsNil)
    c.Assert(b.restore(remotepath, localPath(s.dir+"/output.txt.1"), -1), IsNil)
    output, err := ioutil.ReadFile(s.dir + "/output.txt.1")
    c.Assert(err, IsNil)
    c.Assert(output, DeepEquals, []byte("bar,hello_world"))
    // write something else to output.txt
    c.Assert(ioutil.WriteFile(s.dir+"/output.txt", []byte("bar,again,hello_world"), 0700), IsNil)
    c.Assert(b.backup(localPath(s.dir+"/output.txt"), remotepath), IsNil)
    c.Assert(b.restore(remotepath, localPath(s.dir+"/output.txt.2"), -1), IsNil)
    output, err = ioutil.ReadFile(s.dir + "/output.txt.2")
    c.Assert(err, IsNil)
    c.Assert(output, DeepEquals, []byte("bar,again,hello_world"))
}
