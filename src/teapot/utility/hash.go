package utility

import (
    "bytes"
    "crypto/sha256"
    "encoding/base64"
    "errors"
    "hash"
)

const hashDebug Debug = true

var shaHasher hash.Hash

func init() {
    shaHasher = sha256.New()
}

// Get the hash of a binary chunk. The binary is usually the encoding of an object.
func GetHashOfBytes(buf []byte) []byte {
    shaHasher.Reset()
    _, err := shaHasher.Write(buf)
    if err != nil {
        hashDebug.Panicf("Computer hash error: %v", err.Error())
    }
    return shaHasher.Sum(nil)
}

// Get the hash of a binary chunk and encode the binary hash into base64 encoded String.
// The binary is usually the encoding of an object.
func GetHashOfBytesAndEncode(buf []byte) string {
    hash := GetHashOfBytes(buf)
    return base64.URLEncoding.EncodeToString(hash)
}

// Given a binary chunk and the hash, check if the chunk really hashes to the given hash.
func ValidateEncodedHash(buf []byte, encodedHash string) error {
    actualEncodedHash := GetHashOfBytesAndEncode(buf)
    if actualEncodedHash != encodedHash {
        return hashDebug.Error(errors.New("Hash doesn't match."))
    }
    return nil
}

// Given a binary chunk and the hash, check if the chunk really hashes to the given hash.
func ValidateHash(buf []byte, hash []byte) error {
    actualHash := GetHashOfBytes(buf)
    if !bytes.Equal(actualHash, hash) {
        return hashDebug.Error(errors.New("Hash doesn't match."))
    }
    return nil
}
