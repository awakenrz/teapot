package adaptor

import (
    "launchpad.net/goamz/aws"
    . "launchpad.net/gocheck"
    "testing"
    "time"
)

// Hook up gocheck into the gotest runner.
func Test(t *testing.T) { TestingT(t) }

type S3Suite struct{}
type FakeS3Suite struct{}

var _ = Suite(&S3Suite{})
var _ = Suite(&FakeS3Suite{})

var auth *aws.Auth
var testBucket = "ut_teapot_test"
var testKey = "test_key"
var testTextValue = "hello, world"
var testBinaryValue = []byte("hello, world")

func (s *S3Suite) SetUpSuite(c *C) {
    a, err := EnvAuth()
    if err != nil {
        c.Skip("No aws key provided.")
    }
    auth = &a
}

func (s *S3Suite) TestText(c *C) {
    adaptor := GetAdaptor(auth, testBucket)
    err := adaptor.PutText(testKey, testTextValue)
    c.Assert(err, IsNil)
    textValue, err := adaptor.GetText(testKey)
    c.Assert(err, IsNil)
    c.Assert(textValue, Equals, testTextValue)
}

func (s *S3Suite) TestBinary(c *C) {
    adaptor := GetAdaptor(auth, testBucket)
    err := adaptor.PutBinary(testKey, testBinaryValue)
    c.Assert(err, IsNil)
    binaryValue, err := adaptor.GetBinary(testKey)
    c.Assert(err, IsNil)
    c.Assert(binaryValue, DeepEquals, testBinaryValue)
}

func (s *FakeS3Suite) TestBadRequest(c *C) {
    auth := &aws.Auth{"hello", "world"}
    //    auth.AccessKey="AKIAJQ65GFTNDA5WQROA"
    //    auth.SecretKey="ky0Uk5MOFFPvfOdwiq0hSq2fzsTZK6ZG8gK5QbBW"
    adaptor := GetAdaptor(auth, testBucket)
    for i := 0; i < 10; i++ {
        err := adaptor.PutBinary(testKey, testBinaryValue)
        c.Assert(err, NotNil)
        time.Sleep(2 * time.Second)
    }
}
