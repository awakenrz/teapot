package utility

import (
    "crypto/aes"
    "crypto/cipher"
    "crypto/rand"
    "crypto/rsa"
    "crypto/sha256"
    "crypto/x509"
    "encoding/base64"
    "encoding/hex"
    "errors"
    "io"
)

const cryptoDebug Debug = true

// A writer where plaintext is given and encrypted data is written.
type EncryptWriter struct {
    writer io.Writer
    key    []byte
    cipher cipher.Stream
    iv     []byte
}

// A reader which reads ciphertext from the underlying stream and returns plaintext.
type DecryptReader struct {
    reader io.Reader
    key    []byte
    cipher cipher.Stream
    iv     []byte
}

func KeyFromPassphrase(passphrase string) []byte {
    hasher := sha256.New()
    hasher.Write([]byte(passphrase + "teapot_salt"))
    return hasher.Sum(nil)
}

func KeyFromPassphraseAndEncode(passphrase string) string {
    return hex.EncodeToString(KeyFromPassphrase(passphrase))
}

func GenerateKeyPairAndEncode() (string, string) {
    privateKey, err := rsa.GenerateKey(rand.Reader, 1024)
    if err != nil {
        panic(err.Error())
    }
    encodedPrivateKey := x509.MarshalPKCS1PrivateKey(privateKey)
    encodedPublicKey, err := x509.MarshalPKIXPublicKey(&privateKey.PublicKey)
    if err != nil {
        panic(err.Error())
    }

    encodedPriKey := base64.URLEncoding.EncodeToString(encodedPrivateKey)
    encodedPubKey := base64.URLEncoding.EncodeToString(encodedPublicKey)
    return encodedPriKey, encodedPubKey
}

func NewEncryptWriter(writer io.Writer, key []byte) (*EncryptWriter, error) {
    aesCipher, err := aes.NewCipher(key)
    if err != nil {
        return nil, cryptoDebug.Error(err)
    }
    iv := make([]byte, aesCipher.BlockSize())
    if n, err := io.ReadAtLeast(rand.Reader, iv, aesCipher.BlockSize()); n != aesCipher.BlockSize() || err != nil {
        cryptoDebug.Error(err)
        return nil, cryptoDebug.Error(errors.New("Initiate cipher failed. "))
    }
    //fmt.Println(iv)
    streamCipher := cipher.NewCTR(aesCipher, iv)
    if n, err := writer.Write(iv); n < aesCipher.BlockSize() || err != nil {
        return nil, cryptoDebug.Error(errors.New("Writing IV failed."))
    }
    return &EncryptWriter{
        writer,
        key,
        streamCipher,
        nil,
    }, nil
}

// Write plaintext to the stream and ciphertext is written to the underlying stream
func (s *EncryptWriter) Write(buf []byte) (int, error) {
    des := make([]byte, len(buf))
    s.cipher.XORKeyStream(des, buf)
    n, err := s.writer.Write(des)
    if err != nil {
        return n, cryptoDebug.Error(err)
    }
    return n, nil
}

//
func NewDecryptReader(reader io.Reader, key []byte) (*DecryptReader, error) {
    aesCipher, err := aes.NewCipher(key)
    if err != nil {
        return nil, cryptoDebug.Error(err)
    }
    iv := make([]byte, aesCipher.BlockSize())
    if n, err := io.ReadAtLeast(reader, iv, aesCipher.BlockSize()); n != aesCipher.BlockSize() || err != nil {
        return nil, cryptoDebug.Error(errors.New("Initiate cipher failed. " + err.Error()))
    }
    //fmt.Println(iv)
    streamCipher := cipher.NewCTR(aesCipher, iv)
    return &DecryptReader{
        reader,
        key,
        streamCipher,
        nil,
    }, nil
}

// Read from a ciphertext stream and return plaintext
func (s *DecryptReader) Read(buf []byte) (int, error) {
    n, err := s.reader.Read(buf)
    if n > 0 {
        s.cipher.XORKeyStream(buf[:n], buf[:n])
    }
    if err != nil {
        return n, cryptoDebug.Error(err)
    }
    return n, nil
}
