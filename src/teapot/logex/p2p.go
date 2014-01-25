package logex

import (
    "errors"
    "net"
    "net/http"
    "net/rpc"
    "strings"
    "teapot/log"
    "teapot/utility"
)

const p2pDebug utility.Debug = true

type IP2PLogEx interface {
    GetEntryByEncodedHash(logEx *LogEx, key log.EncodedHash, logEntry *log.LogEntry) error
    GetLastLogEntryInfoOfNode(logEx *LogEx, nodeId log.NodeID, response *log.VersionInfo) error
    GetValue(logEx *LogEx, key log.EncodedHash, object *[]byte) error
}

type p2pLogEx struct{}

// Used to get log entries when S3 is down
func (server *p2pLogEx) GetEntryByEncodedHash(logEx *LogEx, key log.EncodedHash, logEntry *log.LogEntry) error {
    p := logEx.theLog.GetEntryByEncodedHash(key)
    p2pDebug.Debugf("Log entry to send: %+v", p)
    p.CopyTo(logEntry)
    if logEntry == nil {
        return p2pDebug.Error(errors.New("Teapot: Log entry with the specified hash doesn't exist"))
    }
    return nil
}

// Used to check whether there is new update when S3 is down.
func (server *p2pLogEx) GetLastLogEntryInfoOfNode(logEx *LogEx, nodeId log.NodeID, response *log.VersionInfo) error {
    // Notice how to assign to the response.
    versionInfo := logEx.theLog.GetLastEntryInfoOfNode(nodeId)
    if versionInfo != nil {
        logEntry := logEx.theLog.GetEntryByEncodedHash(versionInfo.HashOfUpdate)
        if logEntry != nil && logEntry.EncodedHash() != versionInfo.HashOfUpdate {
            p2pDebug.Panicf("Should not happen: hash doesn't match.")
        }
        p2pDebug.Debugf("%v", logEntry)
        *response = *versionInfo
        return nil
    }
    return nil
}

// Get Value when S3 is down.
func (server *p2pLogEx) GetValue(logEx *LogEx, key log.EncodedHash, object *[]byte) error {
    value, err := logEx.theLog.GetValue(key)
    if err != nil {
        return err
    }
    *object = value
    return nil
}

// Used to get log entries when S3 is down
func (server *LogEx) GetEntryByEncodedHash(key log.EncodedHash, logEntry *log.LogEntry) error {
    return server.p2p.GetEntryByEncodedHash(server, key, logEntry)
}

// Used to check whether there is new update when S3 is down.
func (server *LogEx) GetLastLogEntryInfoOfNode(nodeId log.NodeID, response *log.VersionInfo) error {
    // Notice how to assign to the response.
    return server.p2p.GetLastLogEntryInfoOfNode(server, nodeId, response)
}

// Get Value when S3 is down.
func (server *LogEx) GetValue(key log.EncodedHash, object *[]byte) error {
    return server.p2p.GetValue(server, key, object)
}

func (server *LogEx) startP2P(ipPort string) {
    s := rpc.NewServer()
    s.Register(server)
    s.HandleHTTP("/"+string(server.myNodeId), "/"+string(server.myNodeId)+"_debug")
    parts := strings.Split(ipPort, ":")
    if len(parts) != 2 {
        p2pDebug.Panicf("Malformed ip port information, %v", ipPort)
    }
    listener, err := net.Listen("tcp", ":"+parts[1])
    if err != nil {
        p2pDebug.Panicf("Unable to set up p2p server. %v", err)
    }
    server.listener = listener
    go http.Serve(listener, nil)
}

func (server *LogEx) stopP2P() {
    if server.listener != nil {
        server.listener.Close()
    }
}
