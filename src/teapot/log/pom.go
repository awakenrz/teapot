package log

import (
    "fmt"
)

// TODO Necessary?
type pOM struct {
    NodeId        NodeID
    HashOfUpdate1 EncodedHash
    HashOfUpdate2 EncodedHash
}

func (pom pOM) String() string {
    return fmt.Sprintf("[%v,%v,%v]", pom.NodeId, pom.HashOfUpdate1, pom.HashOfUpdate2)
}

/*
// TODO
func (pom *pOM) handle(log *Log, logEntry *LogEntry) error {
    return nil
}

// TODO
func (pom *pOM) asyncHandle(log *Log, logEntry *LogEntry) error {
    return nil
}*/
