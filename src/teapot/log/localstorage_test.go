package log

import (
    "io/ioutil"
    . "launchpad.net/gocheck"
    "os"
    "teapot/utility"
)

type StorageSuite struct{}

var _ = Suite(&StorageSuite{})

func (s *StorageSuite) TestSnapshotManager(c *C) {
    sm := newSnapshotManager("temp/snapshot")
    // Assume that the previous statement has created temp/snapshot
    os.Mkdir("temp/snapshot/0", 0700)
    os.Mkdir("temp/snapshot/1", 0700)
    c.Assert(sm, NotNil)
    backups, err := sm.GetAllSnapshotFolders()
    c.Assert(err, IsNil)
    c.Assert(backups[0], Equals, "0")
    c.Assert(backups[1], Equals, "1")

    backup, err := sm.GetLastSnapshotFolder()
    c.Assert(err, IsNil)
    c.Assert(backup, Equals, "1")
    os.RemoveAll("temp")
}

func (s *StorageSuite) TestLog(c *C) {
    os.Mkdir("temp", 0700)
    ls := newLogStorage("temp/log.txt")
    c.Assert(ls, NotNil)
    //li := newLogIterator("temp/log.txt")
    os.RemoveAll("temp")
}

func (s *StorageSuite) TestJournal(c *C) {
    os.Mkdir("temp", 0700)
    js := newJournalStorage("temp/journal.txt")
    c.Assert(js, NotNil)
    //ji := newJournalIterator("temp/journal.txt")
    os.RemoveAll("temp")
}

func (s *StorageSuite) TestExist(c *C) {
    c.Assert(existFile("nonexist.txt"), Equals, false)
    c.Assert(existDir("temp"), Equals, false)
    os.Mkdir("temp", 0700)
    c.Assert(existDir("temp"), Equals, true)
    c.Assert(existFile("temp"), Equals, false)
    ioutil.WriteFile("temp/exist.txt", []byte("hello"), 0600)
    c.Assert(existFile("temp/exist.txt"), Equals, true)
    c.Assert(existDir("temp/exist.txt"), Equals, false)
    os.RemoveAll("temp")
}

func (s *StorageSuite) TestValueManager(c *C) {
    os.Mkdir("temp", 0700)
    vm := newValueManager("temp")
    c.Assert(vm, NotNil)
    hash := utility.GetHashOfBytesAndEncode([]byte("hello"))
    err := vm.WriteValue(EncodedHash(hash), []byte("hello"))
    c.Assert(err, IsNil)
    value, err := vm.ReadValue(EncodedHash(hash))
    c.Assert(err, IsNil)
    c.Assert(value, DeepEquals, []byte("hello"))
    os.RemoveAll("temp")
}

func (s *StorageSuite) TestWriteFile(c *C) {
    os.Mkdir("temp", 0700)
    err := writeFile("temp/a.txt", []byte("hello"))
    c.Assert(err, IsNil)
    content, err := ioutil.ReadFile("temp/a.txt")
    c.Assert(err, IsNil)
    c.Assert(content, DeepEquals, []byte("hello"))
    os.RemoveAll("temp")
}

func (s *StorageSuite) TestAppendToFile(c *C) {
    os.Mkdir("temp", 0700)
    err := appendToFile("temp/a.txt", []byte("hello"))
    c.Assert(err, IsNil)
    content, err := ioutil.ReadFile("temp/a.txt")
    c.Assert(err, IsNil)
    c.Assert(content, DeepEquals, []byte("hello"))
    err = appendToFile("temp/a.txt", []byte("hello"))
    c.Assert(err, IsNil)
    content, err = ioutil.ReadFile("temp/a.txt")
    c.Assert(err, IsNil)
    c.Assert(content, DeepEquals, []byte("hellohello"))
    os.RemoveAll("temp")
}
