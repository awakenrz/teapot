package adaptor

//package main

import (
    "io"
    "launchpad.net/goamz/aws"
    "launchpad.net/goamz/s3"
    "sync"
    "teapot/utility"
)

const s3adaptorDebug utility.Debug = true

type S3Adaptor struct {
    s3Adaptor    *s3.S3
    myBucketName string
    allBuckets   map[string]*s3.Bucket
}

var adaptor *S3Adaptor
var once sync.Once

func EnvAuth() (aws.Auth, error) {
    return aws.EnvAuth()
}

func NewAuth(accessKey, secretKey string) *aws.Auth {
    return &aws.Auth{accessKey, secretKey}
}

// make it once
func GetAdaptor(auth *aws.Auth, myBucketName string) *S3Adaptor {
    if adaptor == nil {
        once.Do(func() {
            s3Adaptor := s3.New(*auth, aws.USEast)
            allBuckets := make(map[string]*s3.Bucket)
            adaptor = &S3Adaptor{
                s3Adaptor,
                myBucketName,
                allBuckets,
            }
            myBucket := adaptor.s3Adaptor.Bucket(myBucketName)
            adaptor.allBuckets[myBucketName] = myBucket
        })
    }
    return adaptor
}

func (adaptor *S3Adaptor) PutTextTo(path, key, text string) error {
    bucket := adaptor.getBucket(path)
    err := bucket.Put(key, []byte(text), "text/plain", s3.PublicRead)
    if err != nil {
        s3adaptorDebug.Error(err)
        s3Err, _ := err.(*s3.Error)
        if s3Err != nil && s3Err.Code == "NoSuchBucket" {
            bucket.PutBucket(s3.BucketOwnerFull)
            err = bucket.Put(key, []byte(text), "text/plain", s3.PublicRead)
            if err != nil {
                return s3adaptorDebug.Error(err)
            }
        } else {
            return s3adaptorDebug.Error(err)
        }
    }
    return nil
}

func (adaptor *S3Adaptor) PutText(key, value string) error {
    return adaptor.PutTextTo(adaptor.myBucketName, key, value)
}

// Handle errors such as bucket not exist
func (adaptor *S3Adaptor) PutBinaryTo(path, key string, value []byte) error {
    bucket := adaptor.getBucket(path)
    err := bucket.Put(key, value, "binary/octet-stream", s3.PublicRead)
    if err != nil {
        s3adaptorDebug.Error(err)
        s3Err, _ := err.(*s3.Error)
        if s3Err != nil && s3Err.Code == "NoSuchBucket" {
            bucket.PutBucket(s3.BucketOwnerFull)
            err = bucket.Put(key, value, "binary/octet-stream", s3.PublicRead)
            if err != nil {
                return s3adaptorDebug.Error(err)
            }
        } else {
            return s3adaptorDebug.Error(err)
        }
    }
    return nil
}

func (adaptor *S3Adaptor) PutBinary(key string, value []byte) error {
    return adaptor.PutBinaryTo(adaptor.myBucketName, key, value)
}

func (adaptor *S3Adaptor) GetTextFrom(path, key string) (string, error) {
    bucket := adaptor.getBucket(path)
    buf, err := bucket.Get(key)
    if err != nil {
        return "", s3adaptorDebug.Error(err)
    }
    return string(buf), nil
}

func (adaptor *S3Adaptor) GetText(key string) (string, error) {
    return adaptor.GetTextFrom(adaptor.myBucketName, key)
}

func (adaptor *S3Adaptor) GetBinaryFrom(path, key string) ([]byte, error) {
    bucket := adaptor.getBucket(path)
    return bucket.Get(key)
}

func (adaptor *S3Adaptor) GetBinary(key string) ([]byte, error) {
    return adaptor.GetBinaryFrom(adaptor.myBucketName, key)
}

func (adaptor *S3Adaptor) getBucket(bucketName string) *s3.Bucket {
    bucket := adaptor.allBuckets[bucketName]
    if bucket == nil {
        bucket = adaptor.s3Adaptor.Bucket(bucketName)
        adaptor.allBuckets[bucketName] = bucket
    }
    return bucket
}

func (adaptor *S3Adaptor) PutReader(key string, r io.Reader, length int64, contType string) error {
    return adaptor.PutReaderTo(adaptor.myBucketName, key, r, length, contType)
}

func (adaptor *S3Adaptor) GetReader(key string) (io.ReadCloser, error) {
    return adaptor.GetReaderFrom(adaptor.myBucketName, key)
}

func (adaptor *S3Adaptor) PutReaderTo(path, key string, r io.Reader, length int64, contType string) error {
    bucket := adaptor.getBucket(path)
    err := bucket.PutReader(key, r, length, contType, s3.PublicRead)
    if err != nil {
        s3Err, _ := err.(*s3.Error)
        if s3Err != nil && s3Err.Code == "NoSuchBucket" {
            bucket.PutBucket(s3.BucketOwnerFull)
            err = bucket.PutReader(key, r, length, contType, s3.PublicRead)
            if err != nil {
                return s3adaptorDebug.Error(err)
            }
        } else {
            return s3adaptorDebug.Error(err)
        }
    }
    return nil
}

func (adaptor *S3Adaptor) GetReaderFrom(path, key string) (io.ReadCloser, error) {
    bucket := adaptor.getBucket(path)
    return bucket.GetReader(key)
}
