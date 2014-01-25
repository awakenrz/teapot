package client

import (
    "net/rpc"
    "teapot/conf"
    "teapot/log"
    . "teapot/rpc"
    "teapot/utility"
)

const clientDebug utility.Debug = true

type Client struct {
    addr string
}

func NewClient(config *conf.Config) *Client {
    return &Client{
        "127.0.0.1:" + config.DaemonPort,
    }
}

func (c *Client) GC() error {
    client, err := rpc.Dial("tcp", c.addr)
    if err != nil {
        return clientDebug.Error(err)
    }
    defer client.Close()
    request := GCRequest{}
    var response GCResponse
    if err := client.Call("TeapotServer.GC", request, &response); err != nil {
        return clientDebug.Error(err)
    }
    return nil
}

func (c *Client) LS() ([]log.Key, error) {
    client, err := rpc.Dial("tcp", c.addr)
    if err != nil {
        return nil, clientDebug.Error(err)
    }
    defer client.Close()
    request := LSRequest{}
    var response LSResponse
    if err := client.Call("TeapotServer.LS", request, &response); err != nil {
        return nil, clientDebug.Error(err)
    }
    return response.Keys, nil
}

func (c *Client) Put(key log.Key, value []byte) error {
    client, err := rpc.Dial("tcp", c.addr)
    if err != nil {
        return clientDebug.Error(err)
    }
    defer client.Close()
    request := PutRequest{
        key,
        value,
    }
    var response PutResponse
    if err := client.Call("TeapotServer.Put", request, &response); err != nil {
        return clientDebug.Error(err)
    }
    return nil
}

func (c *Client) Get(key log.Key) ([][]byte, error) {
    client, err := rpc.Dial("tcp", c.addr)
    if err != nil {
        return nil, clientDebug.Error(err)
    }
    defer client.Close()
    request := GetRequest{key}
    var response GetResponse
    if err := client.Call("TeapotServer.Get", request, &response); err != nil {
        return nil, clientDebug.Error(err)
    }
    return response.Value, nil
}

func (c *Client) GetVersion(key log.Key, version int) ([][]byte, error) {
    client, err := rpc.Dial("tcp", c.addr)
    if err != nil {
        return nil, clientDebug.Error(err)
    }
    defer client.Close()
    request := GetVersionRequest{key, version}
    var response GetResponse
    if err := client.Call("TeapotServer.GetVersion", request, &response); err != nil {
        return nil, clientDebug.Error(err)
    }
    return response.Value, nil
}

func (c *Client) GetVersions() ([]string, error) {
    client, err := rpc.Dial("tcp", c.addr)
    if err != nil {
        return nil, clientDebug.Error(err)
    }
    defer client.Close()
    request := VersionRequest{}
    var response VersionResponse
    if err := client.Call("TeapotServer.GetVersions", request, &response); err != nil {
        return nil, clientDebug.Error(err)
    }
    return response.Versions, nil
}

func (c *Client) ChMod(directory log.Dir, key log.SecretKey, readers, writers []log.NodeID) error {
    client, err := rpc.Dial("tcp", c.addr)
    if err != nil {
        return clientDebug.Error(err)
    }
    defer client.Close()
    request := ChangeModeRequest{
        directory,
        key,
        readers,
        writers,
    }
    var response ChangeModeResponse
    if err := client.Call("TeapotServer.ChangeMode", request, &response); err != nil {
        return clientDebug.Error(err)
    }
    return nil
}
