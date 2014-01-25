package main

/*
   Each file is oranized as follow:
   1. The original version is stored as a whole
   2. Every checkpoint is actually the rdiff patch file
   3. Accumulate enough patch until the total size of patch is larger than a new whole file,
   Store the new file again.
   4. A local cache is kept in order to enable rdiff
   5. If the local copy is not in present, just checkout the latest version.
   6. In order to restore to a specific version, first checkout the most recent full version,
   then checkout each patch and apply each one by one

   What need to be stored:
   1. Use hash of remote file path as key to store a summary of the file information.
   2. The file information contains the hash of file, mod date, the key of all versions and a summary (time, patch or full file, etc.) of each version, etc.

   Steps to interact with Depot
   1. Write file content
   2. Update file info
*/

import (
    "errors"
    "flag"
    "fmt"
    "io/ioutil"
    "os"
    "os/exec"
    "path"
    "strconv"
    "strings"
    "teapot/conf"
    "teapot/log"
    "teapot/rpc/client"
    "teapot/rpc/server"
    "teapot/utility"
    "time"
)

const backupotDebug utility.Debug = true

type RemoteFileInfo struct {
    ModeDate     time.Time
    EncodedHash  string
    OriginalName string
    Dir          bool
    Compressed   bool
}

type BackuPOT struct {
    config *conf.Config
    client *client.Client
}

type remotePath string
type localPath string

func NewBackuPOT(config *conf.Config) *BackuPOT {
    return &BackuPOT{
        config,
        client.NewClient(config),
    }
}

func Usage() {
    fmt.Println("Usage: backot backup <localPath> <remotePath>")
    fmt.Println("Usage: backot restore <remotePath> <localPath> <version>")
    fmt.Println("Usage: backot version")
    fmt.Println("Usage: backot chmod <directory> <passphrase> <nodeId>+(r|w) ... ")
    fmt.Println("Usage: backot snapshot")
    fmt.Println("Usage: backot ls")
}

func main() {
    flag.Parse()
    if flag.NArg() < 1 {
        Usage()
        return
    }
    op := flag.Arg(0)
    switch op {
    case "backup":
        localPath := localPath(flag.Arg(1))
        remotePath := remotePath(flag.Arg(2))
        if remotePath == "" {
            Usage()
            return
        }
        config, err := conf.LoadFromFile("teapot.config")
        if err != nil {
            fmt.Println(err.Error())
            return
        }
        b := NewBackuPOT(config)
        if err := b.backup(localPath, remotePath); err != nil {
            fmt.Println(err.Error())
            return
        }
        fmt.Printf("Backup succeeded. %v is backed up to %v\n", localPath, remotePath)
    case "restore":
        remotePath := remotePath(flag.Arg(1))
        localPath := localPath(flag.Arg(2))
        version, err := strconv.ParseInt(flag.Arg(3), 10, 32)
        if err != nil {
            Usage()
            return
        }
        config, err := conf.LoadFromFile("teapot.config")
        if err != nil {
            fmt.Println(err.Error())
            return
        }
        b := NewBackuPOT(config)
        if err := b.restore(remotePath, localPath, int(version)); err != nil {
            fmt.Println(err.Error())
            return
        }
        fmt.Printf("Restore succeeded. %s (v%v) is restored up to %v\n", remotePath, version, localPath)
    case "version":
        //remotePath := remotePath(flag.Arg(1))
        config, err := conf.LoadFromFile("teapot.config")
        if err != nil {
            fmt.Println(err.Error())
            return
        }
        b := NewBackuPOT(config)
        vers, err := b.versions()
        if err != nil {
            fmt.Println(err.Error())
            return
        }
        fmt.Println("-1: Current version")
        for index, ver := range vers {
            fmt.Printf("%v: %v\n", index, ver.Local())
        }
    case "chmod":
        if flag.NArg() < 3 {
            Usage()
            return
        }
        directory := flag.Arg(1)
        newPassprahse := flag.Arg(2)
        readers := make([]log.NodeID, 0)
        writers := make([]log.NodeID, 0)
        for i := 3; i < flag.NArg(); i++ {
            splitter := strings.Index(flag.Arg(i), "+")
            if splitter > 0 {
                nodeId := flag.Arg(i)[:splitter]
                mode := flag.Arg(i)[splitter+1:]
                switch mode {
                case "r":
                    readers = append(readers, log.NodeID(nodeId))
                case "w":
                    writers = append(writers, log.NodeID(nodeId))
                default:
                    Usage()
                    return
                }
            } else {
                Usage()
                return
            }
        }
        key := utility.KeyFromPassphrase(newPassprahse)
        config, err := conf.LoadFromFile("teapot.config")
        if err != nil {
            fmt.Println(err.Error())
            return
        }
        b := NewBackuPOT(config)
        if err := b.chmod(directory, key, readers, writers); err != nil {
            fmt.Println(err.Error())
            return
        }
        fmt.Println("ChMod succeeded.")
    case "snapshot":
        config, err := conf.LoadFromFile("teapot.config")
        if err != nil {
            fmt.Println(err.Error())
            return
        }
        b := NewBackuPOT(config)
        if err := b.snapshot(); err != nil {
            fmt.Println(err.Error())
            return
        }
        fmt.Println("Wait for other clients to respond to your request. You should be able to see the snapshot using version command after it finishes.")
    case "server":
        startOrStop := flag.Arg(1)
        configPath := flag.Arg(2)
        if startOrStop == "start" {
            config, err := conf.LoadFromFile(configPath)
            if err != nil {
                fmt.Println(err.Error())
                panic(err.Error())
            }
            s := server.NewTeapotServer(config)
            fmt.Println("Starting server...")
            server.StartServer(s)
        }
    case "ls":
        config, err := conf.LoadFromFile("teapot.config")
        if err != nil {
            fmt.Println(err.Error())
            return
        }
        b := NewBackuPOT(config)
        list, err := b.ls()
        if err != nil {
            fmt.Println(err.Error())
            return
        }
        for _, file := range list {
            fmt.Printf("%v\n", file)
        }
    default:
        Usage()
    }
}

func (b *BackuPOT) ls() ([]string, error) {
    keys, err := b.client.LS()
    if err != nil {
        return nil, backupotDebug.Error(err)
    }
    files := make([]string, 0)
    for _, key := range keys {
        if strings.HasSuffix(string(key), ".info") {
            files = append(files, strings.TrimRight(string(key), ".info"))
        }
    }
    return files, nil
}

func (b *BackuPOT) chmod(directory string, key []byte, readers, writers []log.NodeID) error {
    if err := b.client.ChMod(log.Dir(directory), log.SecretKey(key), readers, writers); err != nil {
        return backupotDebug.Error(err)
    }
    return nil
}

func splitPath(p localPath) (localPath, string) {
    a, b := path.Split(string(p))
    if a == "" {
        a = "./"
    }
    return localPath(a), b
}

/*
   use tar to pack the local path
   put the file to remote
   put the file info to remote
   use GC to take a snapshot.
*/
func (b *BackuPOT) backup(localPath localPath, remotePath remotePath) error {
    if err := log.ValidateKey(log.Key(remotePath)); err != nil {
        return backupotDebug.Error(err)
    }
    // get file info
    dir, name := splitPath(localPath)
    //localPath = dir + "/" + name
    // compress and read the file
    buf, fileInfo, err := b.prepareFile(dir, name)
    if err != nil {
        return backupotDebug.Error(err)
    }
    if need, err := b.needBackup(remotePath, fileInfo); err != nil {
        return backupotDebug.Error(err)
    } else if !need {
        return backupotDebug.Error(errors.New("No need to backup. No change since last backup."))
    }
    // Put the file to teapot
    if err := b.putRemoteFile(remotePath, buf); err != nil {
        return backupotDebug.Error(err)
    }
    // Put fileInfo
    if err := b.putRemoteFileInfo(remotePath, fileInfo); err != nil {
        return backupotDebug.Error(err)
    }
    return nil
}

func (b *BackuPOT) prepareFile(dir localPath, name string) ([]byte, *RemoteFileInfo, error) {
    if name == "" {
        // back up a dir.
        dir = localPath(strings.TrimRight(string(dir), "/"))
        dir, name = splitPath(dir)
    }
    file, err := os.Stat(string(dir) + name)
    if err != nil {
        return nil, nil, backupotDebug.Error(err)
    }
    // use tar to compress the file
    var buf []byte
    tempDir, err := ioutil.TempDir(os.TempDir(), "temp")
    if err != nil {
        return nil, nil, backupotDebug.Error(err)
    }
    defer os.RemoveAll(tempDir)
    cmd := exec.Command("tar", "-czf", tempDir+"/temp.tar.gz", "-C", string(dir), name)
    if output, err := cmd.CombinedOutput(); err != nil {
        backupotDebug.Debugf(string(output))
        return nil, nil, backupotDebug.Error(err)
    }
    if b, err := ioutil.ReadFile(tempDir + "/temp.tar.gz"); err != nil {
        return nil, nil, backupotDebug.Error(err)
    } else {
        buf = b
    }
    encodedHash := utility.GetHashOfBytesAndEncode(buf)
    // update file info
    fileInfo := RemoteFileInfo{
        file.ModTime(),
        encodedHash, //hash
        name,
        file.IsDir(),
        true,
    }
    return buf, &fileInfo, nil
}

// check if another update need to be taken.
func (b *BackuPOT) needBackup(remotePath remotePath, newFileInfo *RemoteFileInfo) (bool, error) {
    // get remote file info
    fileInfo, err := b.getRemoteFileInfo(remotePath, -1)
    if err != nil {
        // check err kind.
        if !strings.HasPrefix(err.Error(), "The key doesn't exist.") {
            return false, backupotDebug.Error(err)
        } else {
            return true, nil
        }
    }
    if fileInfo.EncodedHash != newFileInfo.EncodedHash {
        return true, nil
    }
    return false, nil
}

/*
   get version
   use tar to unpack it to localpath
*/
// TODO more checks on the path
func (b *BackuPOT) restore(remotePath remotePath, localPath localPath, version int) error {
    if err := log.ValidateKey(log.Key(remotePath)); err != nil {
        return backupotDebug.Error(err)
    }
    dir, name := splitPath(localPath)
    //localPath = dir + "/" + name
    fileInfo, err := b.getRemoteFileInfo(remotePath, version)
    if err != nil {
        return backupotDebug.Error(err)
    }
    file, err := b.getRemoteFile(remotePath, version)
    if err != nil {
        return backupotDebug.Error(err)
    }
    if err := b.writeFile(dir, name, fileInfo, file); err != nil {
        return backupotDebug.Error(err)
    }
    return nil
}

func (b *BackuPOT) writeFile(dir localPath, name string, fileInfo *RemoteFileInfo, buf []byte) error {
    if name == "" {
        name = fileInfo.OriginalName
    }
    tempDir, err := ioutil.TempDir(os.TempDir(), "temp")
    if err != nil {
        return backupotDebug.Error(err)
    }
    if err := ioutil.WriteFile(tempDir+"/temp.tar.gz", buf, 0700); err != nil {
        return backupotDebug.Error(err)
    }
    // untar the file
    cmd := exec.Command("tar", "-xzf", tempDir+"/temp.tar.gz", "-C", tempDir)
    if output, err := cmd.CombinedOutput(); err != nil {
        backupotDebug.Debugf(string(output))
        return backupotDebug.Error(err)
    }
    // rename the file if necessary based on fileinfo
    if dir != "" {
        if err := os.MkdirAll(string(dir), 0700); err != nil {
            return backupotDebug.Error(err)
        }
    }
    if err := os.Rename(tempDir+"/"+fileInfo.OriginalName, string(dir)+name); err != nil {
        return backupotDebug.Error(err)
    }
    return nil
}

// output all version of a file
func (b *BackuPOT) versions() ([]time.Time, error) {
    times, err := b.client.GetVersions()
    if err != nil {
        return nil, backupotDebug.Error(err)
    }
    results := make([]time.Time, 0)
    for _, version := range times {
        num, err := strconv.Atoi(version)
        if err != nil {
            return nil, backupotDebug.Error(err)
        }
        results = append(results, time.Unix(int64(num), 0))
    }
    return results, nil
}

func (b *BackuPOT) snapshot() error {
    err := b.client.GC()
    if err != nil {
        return backupotDebug.Error(err)
    }
    return nil
}

func (b *BackuPOT) putRemoteFileInfo(remotePath remotePath, fileInfo *RemoteFileInfo) error {
    remotePath = remotePath + ".info"
    if err := b.client.Put(log.Key(remotePath), utility.GobEncode(fileInfo)); err != nil {
        return backupotDebug.Error(err)
    }
    return nil
}

func (b *BackuPOT) putRemoteFile(remotePath remotePath, value []byte) error {
    remotePath = remotePath + ".file"
    if err := b.client.Put(log.Key(remotePath), value); err != nil {
        return backupotDebug.Error(err)
    }
    return nil
}

func (b *BackuPOT) getRemoteFileInfo(remotePath remotePath, version int) (*RemoteFileInfo, error) {
    remotePath = remotePath + ".info"
    var values [][]byte
    if version < 0 {
        v, err := b.client.Get(log.Key(remotePath))
        if err != nil {
            return nil, backupotDebug.Error(err)
        }
        values = v
    } else {
        v, err := b.client.GetVersion(log.Key(remotePath), version)
        if err != nil {
            return nil, backupotDebug.Error(err)
        }
        values = v
    }
    var fileInfo RemoteFileInfo
    // TODO enable returning multiple version
    for _, value := range values {
        if err := utility.GobDecode(value, &fileInfo); err != nil {
            return nil, backupotDebug.Error(err)
        }
        return &fileInfo, nil
    }
    return nil, backupotDebug.Error(errors.New("No such file."))
}

func (b *BackuPOT) getRemoteFile(remotePath remotePath, version int) ([]byte, error) {
    remotePath = remotePath + ".file"
    if version < 0 {
        values, err := b.client.Get(log.Key(remotePath))
        if err != nil {
            return nil, backupotDebug.Error(err)
        }
        for _, value := range values {
            return value, nil
        }
        return nil, backupotDebug.Error(errors.New("No such file."))
    }
    values, err := b.client.GetVersion(log.Key(remotePath), version)
    if err != nil {
        return nil, backupotDebug.Error(err)
    }
    for _, value := range values {
        return value, nil
    }
    return nil, backupotDebug.Error(errors.New("No such file."))
}

func init() {
    utility.Register(RemoteFileInfo{})
}
