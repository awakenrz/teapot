package log

import (
    /*"io"
      "teapot/conf"
      "time"*/
    "bufio"
    "encoding/base64"
    "io"
    "io/ioutil"
    "os"
    path_ "path"
    "sort"
    "strconv"
    "strings"
    "teapot/utility"
    "time"
)

const localStorageDebug utility.Debug = true

type iLogStorage interface {
    Append(logEntry *LogEntry) error
}

type iLogIterator interface {
    NextLogEntry() (*LogEntry, error)
}

type iJournalStorage interface {
    Write(entry string) error
}

type iJournalIterator interface {
    NextJournalEntry() (string, error)
}

type iValueManager interface {
    WriteValue(encodedHash EncodedHash, value []byte) error
    ReadValue(encodedHash EncodedHash) ([]byte, error)
}

type iSnapshotManager interface {
    GetAllSnapshotFolders() ([]string, error)
    GetLastSnapshotFolder() (string, error)
    NewSnapshot(log *logInMemory) (string, error)
    ReadSnapshot(folder string) (*logInMemory, error)
}

type logStorage struct {
    logFilePath string
}

type logIterator struct {
    file   *os.File
    reader *bufio.Reader
}

type journalStorage struct {
    logFilePath string
}

type journalIterator struct {
    file   *os.File
    reader *bufio.Reader
}

type valueManager struct {
    valueDir string
}

type snapshotManager struct {
    snapshotDir string
}

func newLogStorage(path string) *logStorage {
    dir := path_.Dir(path)
    if !existDir(dir) {
        if err := os.MkdirAll(dir, 0700); err != nil {
            localStorageDebug.Panicf("%v", err)
        }
    }
    return &logStorage{path}
}

func newLogIterator(path string) *logIterator {
    file, err := os.Open(path)
    if err != nil {
        localStorageDebug.Error(err)
        return nil
    }
    reader := bufio.NewReader(file)
    return &logIterator{file, reader}
}

func newJournalStorage(path string) *journalStorage {
    dir := path_.Dir(path)
    if !existDir(dir) {
        if err := os.MkdirAll(dir, 0700); err != nil {
            localStorageDebug.Panicf("%v", err)
        }
    }
    return &journalStorage{path}
}

func newJournalIterator(path string) *journalIterator {
    file, err := os.Open(path)
    if err != nil {
        localStorageDebug.Error(err)
        return nil
    }
    reader := bufio.NewReader(file)
    return &journalIterator{file, reader}
}

func newValueManager(valueDir string) *valueManager {
    valueDir = path_.Clean(valueDir)
    if !existDir(valueDir) {
        if err := os.MkdirAll(valueDir, 0700); err != nil {
            localStorageDebug.Panicf("%v", err)
        }
    }
    return &valueManager{valueDir}
}

func newSnapshotManager(snapshotDir string) *snapshotManager {
    snapshotDir = path_.Clean(snapshotDir)
    if !existDir(snapshotDir) {
        if err := os.MkdirAll(snapshotDir, 0700); err != nil {
            localStorageDebug.Panicf("%v", err)
        }
    }
    return &snapshotManager{snapshotDir}
}

func (iterator *logIterator) NextLogEntry() (*LogEntry, error) {
    line, err := iterator.reader.ReadString('\n')
    if err != nil {
        if err == io.EOF {
            iterator.file.Close()
            return nil, nil
        }
        return nil, localStorageDebug.Error(err)
    }
    line = strings.TrimRight(line, "\n")
    buf, err := base64.URLEncoding.DecodeString(line)
    if err != nil {
        return nil, err
    }
    return deserializeLogEntry(buf)
}

func (storage *logStorage) Append(logEntry *LogEntry) error {
    return appendToFile(storage.logFilePath, []byte(base64.URLEncoding.EncodeToString(logEntry.serialize())+"\n"))
}

func (iterator *journalIterator) NextJournalEntry() (string, error) {
    line, err := iterator.reader.ReadString('\n')
    if err != nil {
        if err == io.EOF {
            iterator.file.Close()
            return "", nil
        }
        return "", localStorageDebug.Error(err)
    }
    return strings.TrimRight(line, "\n"), nil
}

func (storage *journalStorage) Write(entry string) error {
    localStorageDebug.Debugf("Writing journal entry: %v", entry)
    return appendToFile(storage.logFilePath, []byte(entry+"\n"))
}

func (vm *valueManager) WriteValue(encodedHash EncodedHash, value []byte) error {
    //localStorageDebug.Debugf("Value to be written: %v, %X", encodedHash, value)
    if err := utility.ValidateEncodedHash(value, string(encodedHash)); err != nil {
        return localStorageDebug.Error(err)
    }
    if err := writeFile(vm.valueDir+"/"+string(encodedHash), value); err != nil {
        localStorageDebug.Error(err)
    }
    return nil
}

// add integrity check.
func (vm *valueManager) ReadValue(encodedHash EncodedHash) ([]byte, error) {
    buf, err := ioutil.ReadFile(vm.valueDir + "/" + string(encodedHash))
    if err != nil {
        return nil, localStorageDebug.Error(err)
    }
    return buf, nil
}

// Assumption: backup.Name is a full name
// If not, change the backup.Name to something like sm.snapshotDir + backup.Name
func (sm *snapshotManager) GetAllSnapshotFolders() ([]string, error) {
    snapshotDir, err := os.Open(sm.snapshotDir)
    if err != nil {
        return nil, localStorageDebug.Error(err)
    }
    snapshots, err := snapshotDir.Readdir(-1)
    snapshotFolders := make([]string, 0, len(snapshots))
    if err != nil {
        return nil, localStorageDebug.Error(err)
    }
    for _, snapshot := range snapshots {
        if snapshot.IsDir() {
            snapshotFolders = append(snapshotFolders, snapshot.Name())
        }
    }
    if len(snapshotFolders) == 0 {
        return nil, nil
    }
    sort.Sort(snapshotList(snapshotFolders))
    return snapshotFolders, nil
}

type snapshotList []string

func (snapshots snapshotList) Len() int {
    return len(snapshots)
}

func (snapshots snapshotList) Less(i, j int) bool {
    s1, err := strconv.ParseInt(snapshots[i][strings.LastIndex(snapshots[i], "/")+1:], 10, 64)
    if err != nil {
        localStorageDebug.Panicf("%v", err)
    }
    s2, err := strconv.ParseInt(snapshots[j][strings.LastIndex(snapshots[i], "/")+1:], 10, 64)
    if err != nil {
        localStorageDebug.Panicf("%v", err)
    }
    return s1 < s2
}

func (snapshots snapshotList) Swap(i, j int) {
    snapshots[i], snapshots[j] = snapshots[j], snapshots[i]
}

func (sm *snapshotManager) GetLastSnapshotFolder() (string, error) {
    snapshots, err := sm.GetAllSnapshotFolders()
    if err != nil {
        return "", localStorageDebug.Error(err)
    }
    localStorageDebug.Debugf("Snapshots: %v", snapshots)
    if len(snapshots) == 0 {
        return "", nil
    }
    return snapshots[len(snapshots)-1], nil
}

func (sm *snapshotManager) NewSnapshot(log *logInMemory) (string, error) {
    folder := sm.snapshotDir + "/" + strconv.FormatInt(time.Now().Unix(), 10)
    // mkdir and serialize.
    if err := os.MkdirAll(folder, 0700); err != nil {
        os.RemoveAll(folder)
        return "", localStorageDebug.Error(err)
    }
    if err := utility.GobEncodeIntoFile(folder+"/snapshot.log", log); err != nil {
        // Cleanup and return
        os.RemoveAll(folder)
        return "", localStorageDebug.Error(err)
    }
    return folder, nil
}

func (sm *snapshotManager) ReadSnapshot(folder string) (*logInMemory, error) {
    var log logInMemory
    if err := utility.GobDecodeFromFile(sm.snapshotDir+"/"+folder+"/snapshot.log", &log); err != nil {
        return nil, localStorageDebug.Error(err)
    }
    return &log, nil
}

func writeFile(filename string, content []byte) error {
    file, err := ioutil.TempFile(path_.Dir(filename), path_.Base(filename))
    if err != nil {
        return localStorageDebug.Error(err)
    }
    if _, err = file.Write(content); err != nil {
        return localStorageDebug.Error(err)
    }
    if err := file.Sync(); err != nil {
        return localStorageDebug.Error(err)
    }
    if err := file.Close(); err != nil {
        return localStorageDebug.Error(err)
    }
    if err := os.Rename(file.Name(), filename); err != nil {
        return localStorageDebug.Error(err)
    }
    return nil
}

func appendToFile(filename string, content []byte) error {
    // the path of filename must exist
    file, err := os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
    if err != nil {
        return localStorageDebug.Error(err)
    }
    if _, err := file.Write(content); err != nil {
        return localStorageDebug.Error(err)
    }
    if err := file.Sync(); err != nil {
        return localStorageDebug.Error(err)
    }
    if err := file.Close(); err != nil {
        return localStorageDebug.Error(err)
    }
    return nil
}

func existFile(path string) bool {
    if stat, err := os.Stat(path); err != nil {
        if os.IsNotExist(err) {
            return false
        }
        localStorageDebug.Panicf("%v", err)
    } else if stat.IsDir() {
        return false
    }
    return true
}

func existDir(path string) bool {
    if stat, err := os.Stat(path); err != nil {
        if os.IsNotExist(err) {
            return false
        }
        localStorageDebug.Panicf("%v", err)
    } else if !stat.IsDir() {
        return false
    }
    return true
}
