package logex

import (
    "container/list"
    "encoding/base64"
    "errors"
    "fmt"
    "net"
    "net/rpc"
    "strconv"
    "strings"
    "teapot/adaptor"
    "teapot/conf"
    "teapot/log"
    "teapot/utility"
)

const logexDebug utility.Debug = true

type ILogEx interface {
    BackgroundGossip() bool
}

type nodeIPMap map[log.NodeID]string
type nodeBucketMap map[log.NodeID]string

type LogEx struct {
    nodeBucketMap nodeBucketMap
    nodeIPMap     nodeIPMap
    //logToSync     chan *log.LogEntry
    //valueToSync   chan log.EncodedHash
    myNodeId   log.NodeID
    theAdaptor adaptor.Adaptor
    theLog     log.ILog
    config     *conf.Config
    p2p        IP2PLogEx
    listener   net.Listener
}

func NewLogEx(config *conf.Config, theLog log.ILog) *LogEx {
    nodeBucketMap := make(nodeBucketMap)
    for nodeId, bucket := range config.NodeBucketMap {
        nodeBucketMap[log.NodeID(nodeId)] = bucket
    }
    nodeIPMap := make(nodeIPMap)
    for nodeId, ipPort := range config.NodeIpMap {
        nodeIPMap[log.NodeID(nodeId)] = ipPort
    }
    auth := adaptor.NewAuth(config.AWSAccessKey, config.AWSSecretKey)
    theAdaptor := adaptor.GetAdaptor(auth, nodeBucketMap[log.NodeID(config.MyNodeId)])
    logex := LogEx{
        nodeBucketMap,
        nodeIPMap,
        //	make(chan *log.LogEntry),
        //	make(chan log.EncodedHash),
        log.NodeID(config.MyNodeId),
        theAdaptor,
        theLog,
        config,
        &p2pLogEx{},
        nil,
    }
    logex.startP2P(config.IPPort)
    return &logex
}

func (logex *LogEx) BackgroundGossip() bool {
    flag := false
    // TODO just choose a random one to communicate.
    for nodeId := range logex.nodeBucketMap {
        // Introduce blacklist mechanism.
        if logex.theLog.Blocked(nodeId) {
            continue
        }
        latestVersionInfo, err := logex.anyNewLogEntriesOfNode(nodeId)
        if err != nil {
            logexDebug.Error(errors.New(fmt.Sprintf("Error when trying to get new updates information: %v", err)))
        }
        if latestVersionInfo != nil {
            flag = true
            logexDebug.Debugf("New updates from node %v of hash %v and accept stamp %v\n", nodeId, latestVersionInfo.HashOfUpdate, latestVersionInfo.AcceptStamp)
            if err := logex.antiEntropy(nodeId, *latestVersionInfo); err != nil {
                logexDebug.Error(errors.New(fmt.Sprintf("Error when trying to fetch log(key: %v, timestamp: %v) from %v.\n%v\n", latestVersionInfo.HashOfUpdate, latestVersionInfo.AcceptStamp, nodeId, err)))
            }
        }
    }
    return flag
}

func (logex *LogEx) antiEntropy(nodeId log.NodeID, versionInfo log.VersionInfo) error {
    targetLogEntry, err := logex.getEntryByEncodedHashRemotely(nodeId, versionInfo.HashOfUpdate)
    if err != nil {
        return err
    }
    /*
       if logex.theLog.HasLogEntry(targetLogEntry.NodeId, targetLogEntry.EncodedHash()) {
           logexDebug.Panicf("This update is supposed to be new. %+v\n%+v", logex.myNodeId, targetLogEntry)
       }*/
    logEntryStack := list.New()
    logEntryStack.PushBack(targetLogEntry)
    // Get all dependency of this log and put them in a stack.
    // The assumption is we can iterate the stack and simultaneously insert element to it.
    for e := logEntryStack.Front(); e != nil; e = e.Next() {
        if logEntry, ok := e.Value.(*log.LogEntry); ok {
            for nodeId, versionInfo := range logEntry.DVV {
                if !logex.theLog.HasLogEntry(nodeId, versionInfo.HashOfUpdate) {
                    newLogEntry, err := logex.getEntryByEncodedHashRemotely(nodeId, versionInfo.HashOfUpdate)
                    if err != nil {
                        return logexDebug.Error(err)
                    }
                    logexDebug.Debugf("Log entry received: %v", newLogEntry)
                    if versionInfo.HashOfUpdate != newLogEntry.EncodedHash() {
                        return logexDebug.Error(errors.New("Bad dependency"))
                    }
                    logEntryStack.PushBack(newLogEntry)
                }
            }
        } else {
            // should not happen. This means non-logentry is in the stack.
        }
    }
    flags := make(map[log.EncodedHash]bool)
    // Pop the stack and commit changes one by one
    for e := logEntryStack.Back(); e != nil; e = e.Prev() {
        if logEntry, ok := e.Value.(*log.LogEntry); ok {
            // If the logEntry is an update, needs to make sure that the value is already there.
            /*     if logex.theLog.HasLogEntry(logEntry.NodeId, logEntry.EncodedHash()) {
                   logexDebug.Panicf("This update is supposed to be new. %+v\n%+v", logex.myNodeId, logEntry)
               }*/
            encodedHash := logEntry.EncodedHash()
            if _, ok := flags[encodedHash]; !ok {
                if update, ok := logEntry.Message.(*log.Update); ok {
                    value, err := logex.getValueRemotely(logEntry.NodeId, update.HashOfValue)
                    if err != nil {
                        return logexDebug.Error(err)
                    }
                    // persist value locally.
                    if err := logex.theLog.WriteValue(update.HashOfValue, value); err != nil {
                        return logexDebug.Error(err)
                    }
                }
                if err := logex.theLog.Commit(logEntry); err != nil {
                    return logexDebug.Error(err)
                }
                if err := logex.theLog.AsyncHandle(logEntry); err != nil {
                    logexDebug.Debugf(err.Error())
                    panic(err.Error())
                    // TODO retry?
                }
                flags[encodedHash] = true
            }
        } else {
            logexDebug.Panicf("should not happen. This means non-logentry is in the stack.")
        }
    }
    return nil
}

func (logex *LogEx) getEntryByHashRemotely(nodeId log.NodeID, hash []byte) (*log.LogEntry, error) {
    encodedHash := log.EncodedHash(base64.URLEncoding.EncodeToString(hash))
    return logex.getEntryByEncodedHashRemotely(nodeId, encodedHash)
}

// has p2p support.
func (logex *LogEx) getEntryByEncodedHashRemotely(nodeId log.NodeID, encodedHash log.EncodedHash) (*log.LogEntry, error) {
    bucketName, ok := logex.nodeBucketMap[nodeId]
    if !ok {
        return nil, logexDebug.Error(errors.New("not sure where to find this node. configuration not complete."))
    }
    for i := 0; i < 3; i++ {
        logEntryBinary, err := logex.theAdaptor.GetBinaryFrom(bucketName, string(encodedHash))
        if err == nil {
            var logEntry log.LogEntry
            err = utility.GobDecode(logEntryBinary, &logEntry)
            if err != nil {
                return nil, err
            }
            return &logEntry, nil
        }
    }
    // Try to fetch the update in p2p mode.
    return logex.p2pGetEntryByEncodedHash(nodeId, encodedHash)
}

func (logex *LogEx) getValueRemotely(nodeId log.NodeID, encodedHash log.EncodedHash) ([]byte, error) {
    bucketName, ok := logex.nodeBucketMap[nodeId]
    if !ok {
        return nil, logexDebug.Error(errors.New("not sure where to find this node. configuration not complete."))
    }
    for i := 0; i < 3; i++ {
        value, err := logex.theAdaptor.GetBinaryFrom(bucketName, string(encodedHash))
        if err == nil {
            return value, nil
        }
    }
    return logex.p2pGetValue(nodeId, encodedHash)
}

// support p2p mode.
func (logex *LogEx) anyNewLogEntriesOfNode(nodeId log.NodeID) (*log.VersionInfo, error) {
    bucketName, ok := logex.nodeBucketMap[nodeId]
    if !ok {
        return nil, logexDebug.Error(errors.New("not sure where to find this node. configuration not complete."))
    }
    for i := 0; i < 3; i++ {
        latestUpdate, err := logex.theAdaptor.GetTextFrom(bucketName, string(nodeId)+".latestUpdate")
        if err == nil {
            component := strings.Split(latestUpdate, ",")
            if len(component) != 2 {
                return nil, logexDebug.Error(errors.New("Malformed update information."))
            }
            latestAcceptStamp, err := strconv.ParseInt(component[0], 10, 64)
            if err != nil {
                return nil, logexDebug.Error(err)
            }
            if !logex.theLog.Observed(nodeId, log.Timestamp(latestAcceptStamp)) {
                latestLogEntryHash := component[1]
                return &log.VersionInfo{log.Timestamp(latestAcceptStamp), log.EncodedHash(latestLogEntryHash)}, nil
            } else {
                // TODO add staleness time out check here.
            }
            // No newer updates
            return nil, nil
        }
    }

    // respond in p2p mode
    return logex.p2pAnyNewLogEntriesOfNode(nodeId)
}

// get a log entry in p2p mode.
func (logex *LogEx) p2pGetEntryByEncodedHash(nodeId log.NodeID, key log.EncodedHash) (*log.LogEntry, error) {
    client, err := rpc.DialHTTPPath("tcp", logex.nodeIPMap[nodeId], "/"+string(nodeId))
    if err != nil {
        return nil, nil
    }
    defer client.Close()
    var logEntry log.LogEntry
    if err := client.Call("LogEx.GetEntryByEncodedHash", key, &logEntry); err != nil {
        return nil, err
    }
    return &logEntry, nil
}

// check if there are new updates in p2p mode.
func (logex *LogEx) p2pAnyNewLogEntriesOfNode(nodeId log.NodeID) (*log.VersionInfo, error) {
    client, err := rpc.DialHTTPPath("tcp", logex.nodeIPMap[nodeId], "/"+string(nodeId))
    if err != nil {
        return nil, err
    }
    defer client.Close()
    var response log.VersionInfo
    response.AcceptStamp = -1
    if err := client.Call("LogEx.GetLastLogEntryInfoOfNode", nodeId, &response); err != nil {
        return nil, err
    }
    if response.AcceptStamp != -1 {
        if logex.theLog.Observed(nodeId, response.AcceptStamp) {
            return nil, nil
        }
        return &response, nil
    }
    return nil, nil
}

// get value in p2p mode.
func (logex *LogEx) p2pGetValue(nodeId log.NodeID, key log.EncodedHash) ([]byte, error) {
    client, err := rpc.DialHTTPPath("tcp", logex.nodeIPMap[nodeId], "/"+string(nodeId))
    if err != nil {
        return nil, err
    }
    defer client.Close()
    var value []byte
    if err := client.Call("LogEx.GetValue", key, &value); err != nil {
        return nil, err
    }
    return value, nil
}
