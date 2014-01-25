package server

import (
    "net"
    "net/rpc"
    "teapot"
    "teapot/conf"
    . "teapot/rpc"
    "teapot/utility"
)

const serverDebug utility.Debug = true

type TeapotServer struct {
    port     string
    teapot   teapot.ITeapot
    listener net.Listener
}

func NewTeapotServer(config *conf.Config) *TeapotServer {
    t := teapot.NewTeapot(config)
    return &TeapotServer{
        config.DaemonPort,
        t,
        nil,
    }
}

func StartServer(server *TeapotServer) {
    s := rpc.NewServer()
    s.Register(server)
    listener, err := net.Listen("tcp", ":"+server.port)
    if err != nil {
        panic(err.Error())
    }
    server.listener = listener
    s.Accept(listener)
}

func AsyncStartServer(server *TeapotServer) {
    s := rpc.NewServer()
    s.Register(server)
    listener, err := net.Listen("tcp", ":"+server.port)
    if err != nil {
        panic(err.Error())
    }
    server.listener = listener
    go s.Accept(listener)
}

// TODO
func (s *TeapotServer) StopServer() {
    if s.listener != nil {
        s.listener.Close()
    }
}

func (s *TeapotServer) GC(request GCRequest, response *GCResponse) error {
    err := s.teapot.GC()
    if err != nil {
        response.Err = err
        return serverDebug.Error(err)
    }
    return nil
}

func (s *TeapotServer) LS(request LSRequest, response *LSResponse) error {
    keys := s.teapot.LS()
    response.Keys = keys
    return nil
}

func (s *TeapotServer) GetVersions(request VersionRequest, response *VersionResponse) error {
    versions, err := s.teapot.GetVersions()
    if err != nil {
        return serverDebug.Error(err)
    }
    response.Versions = versions
    return nil
}

func (s *TeapotServer) Get(request GetRequest, response *GetResponse) error {
    values, err := s.teapot.Get(request.Key)
    if err != nil {
        return serverDebug.Error(err)
    }
    response.Value = values
    return nil
}

func (s *TeapotServer) GetVersion(request GetVersionRequest, response *GetResponse) error {
    values, err := s.teapot.GetVersion(request.Key, request.Version)
    if err != nil {
        return serverDebug.Error(err)
    }
    response.Value = values
    return nil
}

func (s *TeapotServer) Put(request PutRequest, response *PutResponse) error {
    err := s.teapot.Put(request.Key, request.Value)
    if err != nil {
        response.Err = err
        return serverDebug.Error(err)
    }
    return nil
}

func (s *TeapotServer) ChangeMode(request ChangeModeRequest, response *ChangeModeResponse) error {
    err := s.teapot.ChangeMode(request.Directory, request.SecretKey, request.Readers, request.Writers)
    if err != nil {
        response.Err = err
        return serverDebug.Error(err)
    }
    return nil
}
