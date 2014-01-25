package utility

import (
    "bytes"
    "encoding/gob"
    "os"
)

const gobDebug Debug = true

func Register(value interface{}) {
    gob.Register(value)
}

// Using gob encoding to encode an object and return the binary encoded result.
func GobEncode(value interface{}) []byte {
    buf := bytes.NewBuffer(make([]byte, 0, 1024))
    encoder := gob.NewEncoder(buf)
    // encode unknown type might cause some error
    err := encoder.Encode(value)
    if err != nil {
        gobDebug.Panicf("Failed to encode a value: %+v\n%v\n", value, err)
    }
    return buf.Bytes()
}

// Using gob encoding to decode the given binary chunk into an object.
func GobDecode(buffer []byte, value interface{}) error {
    buf := bytes.NewBuffer(buffer)
    decoder := gob.NewDecoder(buf)
    err := decoder.Decode(value)
    if err != nil {
        return gobDebug.Error(err)
    }
    return nil
}

// Using gob encoding to encode an object into a file.
func GobEncodeIntoFile(filename string, object interface{}) error {
    file, err := os.Create(filename)
    if err != nil {
        return gobDebug.Error(err)
    }
    defer file.Close()
    encoder := gob.NewEncoder(file)
    if err := encoder.Encode(object); err != nil {
        return gobDebug.Error(err)
    }
    if err := file.Sync(); err != nil {
        return gobDebug.Error(err)
    }
    return nil
}

// Using encoding to decode the content of the file into object.
func GobDecodeFromFile(filename string, object interface{}) error {
    file, err := os.Open(filename)
    if err != nil {
        // Might be caused by file does not exist
        return gobDebug.Error(err)
    }
    defer file.Close()
    decoder := gob.NewDecoder(file)
    if err := decoder.Decode(object); err != nil {
        return gobDebug.Error(err)
    }
    return nil
}
