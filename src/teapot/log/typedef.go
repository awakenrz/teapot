package log

import (
    "fmt"
    "sort"
    "strings"
)

type Key string
type EncodedHash string
type NodeID string
type Timestamp int64
type Dir string
type SecretKey []byte

type hash []byte
type path string
type signature []byte

func (sig signature) String() string {
    return fmt.Sprintf("%X", []byte(sig))
}

type versionInfo struct {
    AcceptStamp  Timestamp
    HashOfUpdate EncodedHash
}

func (vi versionInfo) String() string {
    return fmt.Sprintf("[%v,%v]", vi.AcceptStamp, vi.HashOfUpdate)
}

type VersionInfo versionInfo

func (vi VersionInfo) String() string {
    return fmt.Sprintf("[%v,%v]", vi.AcceptStamp, vi.HashOfUpdate)
}

type ipPortType string
type bucketType string
type encodedEncryptedSecretKey string

/*
   Local version vector.
*/
type versionVector map[NodeID]versionInfo

func (vv versionVector) String() string {
    results := make([]string, len(vv))
    i := 0
    for nodeId, versionInfo := range vv {
        results[i] = fmt.Sprintf("[%v:%v]", nodeId, versionInfo)
        i++
    }
    sort.Strings(results)
    return strings.Join(results, ",")
}

/* Type Definition starts here */
type blackList map[NodeID]*pOM

func (bl blackList) String() string {
    results := make([]string, len(bl))
    i := 0
    for nodeId, pom := range bl {
        results[i] = fmt.Sprintf("[%v:%v]", nodeId, pom)
        i++
    }
    sort.Strings(results)
    return strings.Join(results, ",")
}

/*
   Log indexed by each node.
   For each node, the each log entries are sorted by acceptstamp.
*/
type sequentialLog map[NodeID][]*LogEntry

/*
   A collection of log entries node id.
*/
type entryCollectionByNode map[NodeID]*LogEntry

/*
   The latest update for each object
*/
type checkpoint map[Key]entryCollectionByNode

type logIndexedByHash map[EncodedHash]*LogEntry

type entryNodeMap map[EncodedHash]NodeID

type nodeBucketMap map[NodeID]bucketType
type NodeBucketMap nodeBucketMap

type principalList map[NodeID]encodedEncryptedSecretKey

func (pl principalList) String() string {
    results := make([]string, len(pl))
    i := 0
    for nodeId, key := range pl {
        results[i] = fmt.Sprintf("[%v:%v]", nodeId, key)
        i++
    }
    sort.Strings(results)
    return strings.Join(results, ",")
}

type keyInfo struct {
    Key         SecretKey
    AcceptStamp Timestamp
    Writers     map[NodeID]bool
}
