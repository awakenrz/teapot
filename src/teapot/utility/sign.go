package utility

import (
    "crypto"
    "crypto/rand"
    "crypto/rsa"
    "errors"
)

const signDebug Debug = true

// Given a binary chunk and a private key, compute the signature of the binary chunk.
func SignBinary(buf []byte, privateKey *rsa.PrivateKey) ([]byte, error) {
    if privateKey == nil {
        return nil, signDebug.Error(errors.New("Private key is nil"))
    }
    hashOfMessage := GetHashOfBytes(buf)
    signature, err := rsa.SignPKCS1v15(rand.Reader, privateKey, crypto.SHA256, hashOfMessage)
    if err != nil {
        return nil, signDebug.Error(err)
    }
    signDebug.Debugf("Signing signature: %s,%X,%X", buf, hashOfMessage, signature)
    return signature, nil
}

// Given a binary chunk, a private key and the signature, check if the signature matches.
func ValidateSignature(buf []byte, publicKey *rsa.PublicKey, signature []byte) error {
    if publicKey == nil {
        return signDebug.Error(errors.New("Public key is nil"))
    }
    hash := GetHashOfBytes(buf)
    signDebug.Debugf("Verify signature: %s,%X,%X", buf, hash, signature)
    if err := rsa.VerifyPKCS1v15(publicKey, crypto.SHA256, hash, signature); err != nil {
        return signDebug.Error(err)
    }
    return nil
}
