package rpc

import (
    "teapot/log"
)

type GetRequest struct {
    Key log.Key
}

type GetResponse struct {
    Value [][]byte
}

type GetVersionRequest struct {
    Key     log.Key
    Version int
}

type PutRequest struct {
    Key   log.Key
    Value []byte
}

type PutResponse struct {
    Err error
}

type VersionRequest struct {
}

type VersionResponse struct {
    Versions []string
}

type GCRequest struct {
}

type GCResponse struct {
    Err error
}

type LSRequest struct {
}

type LSResponse struct {
    Keys []log.Key
}

type ChangeModeRequest struct {
    Directory log.Dir
    SecretKey log.SecretKey
    Readers   []log.NodeID
    Writers   []log.NodeID
}

type ChangeModeResponse struct {
    Err error
}
