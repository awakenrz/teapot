package log

import (
    "crypto/rand"
    "crypto/rsa"
    "encoding/base64"
    "errors"
    "fmt"
    path_ "path"
    "regexp"
    "strings"
    "teapot/utility"
)

const utilityDebug utility.Debug = true

func ValidateKey(key Key) error {
    if len(string(key)) > 80 {
        return utilityDebug.Error(errors.New(fmt.Sprintf("The key should not be longer than 80 chars, %v", key)))
    }
    matched, err := regexp.MatchString("^[[:alnum:]_]+/[[:alnum:]_]+/([[:alnum:]\\._]+/)*[[:alnum:]\\._]+$", string(key))
    if err != nil {
        return utilityDebug.Error(err)
    } else if !matched {
        return utilityDebug.Error(errors.New(fmt.Sprintf("The key should be formed as nodeId/directory/path. nodeId/directory should only contain _, letters and digits. Path can further contain dots in it. %v", key)))
    }
    return nil
}

// check format each key, especially path.
// The format of the key should be ownerNodeId/dir/key
func splitKey(key Key) (NodeID, Dir, path, error) {
    key = Key(path_.Clean(string(key)))
    if err := ValidateKey(key); err != nil {
        return "", "", "", utilityDebug.Error(err)
    }
    parts := strings.SplitN(string(key), "/", 3)
    if len(parts) != 3 {
        return "", "", "", utilityDebug.Error(errors.New(fmt.Sprintf("The key should be formed as nodeId/directory/path. nodeId/directory should only contain _, letters and digits. Path can further contain dots in it. %v", key)))
    }
    return NodeID(parts[0]), Dir(parts[1]), path(parts[2]), nil
}

// Assume that roq is base64 url encoded.
// Assume that rsa encryption is used.
func extractKey(encodedEncryptedKey encodedEncryptedSecretKey, privateKey *rsa.PrivateKey, roq string) ([]byte, error) {
    encryptedKey, err := base64.URLEncoding.DecodeString(string(encodedEncryptedKey))
    if err != nil {
        return nil, utilityDebug.Error(err)
    }
    key, err := rsa.DecryptPKCS1v15(rand.Reader, privateKey, encryptedKey)
    if err != nil {
        return nil, utilityDebug.Error(err)
    }
    if err := utility.ValidateEncodedHash(key, roq); err != nil {
        return nil, utilityDebug.Error(err)
    }
    return key, nil
}

func getPartialVirtualNodeId(nodeId NodeID, hashOfForkEntry EncodedHash) NodeID {
    return NodeID(string(nodeId) + "." + string(hashOfForkEntry))
}

// Given a virtual node id, no matter full or partial,
// the first part (before the first comma if any) is the node id.
// node id is used when fetch updates from remote and black listing a node.
// Assumption: nodeId should not contain any "."
func getNodeIdFromVirtualNodeId(virtualNodeId NodeID) NodeID {
    index := strings.Index(string(virtualNodeId), ".")
    if index == -1 {
        return virtualNodeId
    }
    if index < 1 {
        utilityDebug.Panicf("Invalid input: %v", virtualNodeId)
    }
    return virtualNodeId[0:index]
}

// TODO Can be optimized, test case for this.
// Each update will only carry version vector entries that has been changed since last update
// This func will tries to build the full version vector recursively based on the partial information carried in DVV of the update.
func buildFullDvv(log *Log, dvv versionVector, dVV versionVector) versionVector {
    //fmt.Println(logEntry)
    for _, versionInfo := range dvv {
        encodedHashOfLogEntry := versionInfo.HashOfUpdate
        virtualNodeId, ok := log.memLog.EntryNodeMap[encodedHashOfLogEntry]
        //fmt.Println(entryNodeMap)
        if !ok {
            updateDebug.Panicf("Should not happen")
        }
        if versionInfo.AcceptStamp > dVV[virtualNodeId].AcceptStamp {
            dVV[virtualNodeId] = versionInfo
        }
        dependentLogEntry := log.GetEntryByEncodedHash(versionInfo.HashOfUpdate)
        if dependentLogEntry == nil {
            updateDebug.Panicf("Should not happen: hasn't seen dependent log entry")
        }
        dVV = buildFullDvv(log, dependentLogEntry.DVV, dVV)
    }
    return dVV
}

