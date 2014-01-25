package adaptor

import "io"

type Adaptor interface {
    //    SetMyBucket(bucketName string)
    PutText(key, value string) error
    PutBinary(key string, value []byte) error
    PutTextTo(path, key, value string) error
    PutBinaryTo(path, key string, value []byte) error
    GetText(key string) (string, error)
    GetBinary(key string) ([]byte, error)
    GetTextFrom(path, key string) (string, error)
    GetBinaryFrom(path, key string) ([]byte, error)
    PutReader(key string, r io.Reader, length int64, contType string) error
    GetReader(key string) (rc io.ReadCloser, err error)
    PutReaderTo(path, key string, r io.Reader, length int64, contType string) error
    GetReaderFrom(path, key string) (rc io.ReadCloser, err error)
}
