package utility

import (
    "bytes"
    "crypto/rand"
    "crypto/rsa"
    "encoding/hex"
    . "launchpad.net/gocheck"
    "os"
    "testing"
)

// Hook up gocheck into the gotest runner.
func Test(t *testing.T) { TestingT(t) }

type S struct{}

var _ = Suite(&S{})

type TestType struct {
    A, B int
    C, D int64
    E, F string
    G, H []byte
}

var input = TestType{1, 2, 3, 4, "hello", "world", []byte{1, 2, 3}, []byte{4, 5, 6}}
var hashInput = []byte("hello, world")
var expectedHash = []byte{0x9, 0xca, 0x7e, 0x4e, 0xaa, 0x6e, 0x8a, 0xe9, 0xc7, 0xd2, 0x61, 0x16, 0x71, 0x29, 0x18, 0x48, 0x83, 0x64, 0x4d, 0x7, 0xdf, 0xba, 0x7c, 0xbf, 0xbc, 0x4c, 0x8a, 0x2e, 0x8, 0x36, 0xd, 0x5b}
var wrongHash = []byte{0x9, 0xca, 0x7e, 0x4e, 0xaa, 0x6e, 0x8a, 0xe9, 0xc7, 0xd2, 0x61, 0x16, 0x71, 0x29, 0x18, 0x48, 0x83, 0x64, 0x4d, 0x7, 0xdf, 0xba, 0x7c, 0xbf, 0xbc, 0x4c, 0x8a, 0x2e, 0x8, 0x36, 0xd, 0x5c}
var expectedEncodedHash = "Ccp-TqpuiunH0mEWcSkYSINkTQffuny_vEyKLgg2DVs="
var wrongEncodedHash = "Ccp-TqpuiunH0mEWcSkYSINkTQffuny_vEyKLgg2DVsb"

func (s *S) SetUpSuite(c *C) {
    Register(TestType{})
}

func (s *S) TearDownSuite(c *C) {
    os.Remove("gob.test")
}

func (s *S) TestGob(c *C) {
    var output TestType
    buf := GobEncode(input)
    err := GobDecode(buf, &output)
    c.Assert(err, IsNil)
    c.Assert(output.A, Equals, input.A)
    c.Assert(output.B, Equals, input.B)
    c.Assert(output.C, Equals, input.C)
    c.Assert(output.D, Equals, input.D)
}

func (s *S) TestGobIntoFile(c *C) {
    err := GobEncodeIntoFile("gob.test", input)
    c.Assert(err, IsNil)
    var output TestType
    err = GobDecodeFromFile("gob.test", &output)
    c.Assert(err, IsNil)
    c.Assert(output.A, Equals, input.A)
    c.Assert(output.B, Equals, input.B)
    c.Assert(output.C, Equals, input.C)
    c.Assert(output.D, Equals, input.D)
}

func (s *S) TestGetHash(c *C) {
    buf := GetHashOfBytes(hashInput)
    c.Assert(buf, DeepEquals, expectedHash)
}

func (s *S) TestGetEncodedHash(c *C) {
    buf := GetHashOfBytesAndEncode(hashInput)
    c.Assert(buf, Equals, expectedEncodedHash)
}

func (s *S) TestValidateHash(c *C) {
    err := ValidateHash(hashInput, expectedHash)
    c.Assert(err, IsNil)
    err = ValidateHash(hashInput, wrongHash)
    c.Assert(err.Error(), Equals, "Hash doesn't match.")
}

func (s *S) TestValidateEncodedHash(c *C) {
    err := ValidateEncodedHash(hashInput, expectedEncodedHash)
    c.Assert(err, IsNil)
    err = ValidateEncodedHash(hashInput, wrongEncodedHash)
    c.Assert(err.Error(), Equals, "Hash doesn't match.")
}

func (s *S) TestSignature(c *C) {
    key, err := rsa.GenerateKey(rand.Reader, 1024)
    c.Assert(err, IsNil)
    buf, err := SignBinary(hashInput, key)
    c.Assert(err, IsNil)
    err = ValidateSignature(hashInput, &key.PublicKey, buf)
    c.Assert(err, IsNil)
    buf[0] = buf[0] ^ 0xff
    err = ValidateSignature(hashInput, &key.PublicKey, buf)
    c.Assert(err.Error(), Equals, "crypto/rsa: verification error")
}

func (s *S) TestPassphrase(c *C) {
    c.Assert(hex.EncodeToString(KeyFromPassphrase("123456")), Equals, "c29cddf923b7049746f0ec6695e4e8eb373de491531928c830b3f31741ea6211")
}

func (s *S) TestCrypto(c *C) {
    key := KeyFromPassphrase("123456")
    buffer := new(bytes.Buffer)
    encrypter, err := NewEncryptWriter(buffer, key)
    c.Assert(err, IsNil)
    _, err = encrypter.Write(expectedHash)
    c.Assert(err, IsNil)
    decrypter, err := NewDecryptReader(buffer, key)
    c.Assert(err, IsNil)
    plaintext := new(bytes.Buffer)
    _, err = plaintext.ReadFrom(decrypter)
    c.Assert(err, IsNil)
    c.Assert(plaintext.Bytes(), DeepEquals, expectedHash)
}

func (s *S) TestLogger(c *C) {
    var debug Debug = true
    debug.Debugf("This is a test")
    debug = false
    debug.Debugf("This is another test")
}
