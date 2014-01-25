package conf

import (
    . "crypto/rand"
    "crypto/rsa"
    "crypto/sha512"
    "crypto/x509"
    "encoding/base64"
    "encoding/hex"
    "encoding/json"
    "io"
    "io/ioutil"
    "math/rand"
    "os"
    "strconv"
    "teapot/utility"
    "time"
)

const confDebug utility.Debug = true

type Configuration struct {
    Property      map[string]string
    NodeBucketMap map[string]string
    NodeIpMap     map[string]string
    PublicKeys    map[string]string
    PrivateKey    string
}

type NodeInfo struct {
    NodeId     string
    NodeBucket string
    NodeIPPort string
    PublicKey  string
}

type Config struct {
    LogPath     string
    JournalPath string
    SnapshotDir string
    ValueDir    string

    MyNodeId     string
    MyBucketName string
    IPPort       string
    DaemonPort   string

    AESKey       []byte
    AWSAccessKey string
    AWSSecretKey string

    NodeBucketMap map[string]string
    NodeIpMap     map[string]string
    PrivateKey    *rsa.PrivateKey
    PublicKeys    map[string]*rsa.PublicKey
}

func LoadFromFile(filename string) (*Config, error) {
    var logPath string
    var journalPath string
    var snapshotDir string
    var valueDir string

    var myNodeId string
    var myBucketName string
    var ipPort string
    var daemonPort string

    var aesKey []byte
    var awsAccessKey string
    var awsSecretKey string

    var nodeBucketMap map[string]string
    var nodeIpMap map[string]string
    var privateKey *rsa.PrivateKey
    var publicKeys map[string]*rsa.PublicKey

    config := ReadConfigFile(filename)
    confDebug.Debugf("%+v", config)

    if config.Property == nil || config.NodeIpMap == nil || config.NodeBucketMap == nil || config.PublicKeys == nil {
        confDebug.Panicf("Configuration file is not complete.\n")
    }
    if _logPath, ok := config.Property["LogPath"]; !ok {
        confDebug.Panicf("Log file path not set up yet.\n")
    } else {
        logPath = _logPath
    }
    if _journalPath, ok := config.Property["JournalPath"]; !ok {
        confDebug.Panicf("Journal file path not set up yet.\n")
    } else {
        journalPath = _journalPath
    }
    if _valueDir, ok := config.Property["ValueDir"]; !ok {
        confDebug.Panicf("Value directory not set up yet.\n")
    } else {
        valueDir = _valueDir
    }
    if _snapshotDir, ok := config.Property["SnapshotDir"]; !ok {
        confDebug.Panicf("Snapshot directory not set up yet.\n")
    } else {
        snapshotDir = _snapshotDir
    }

    if _myNodeId, ok := config.Property["MyNodeId"]; !ok {
        confDebug.Panicf("Node Id not set up yet.\n")
    } else {
        myNodeId = _myNodeId
    }
    if _myBucketName, ok := config.Property["MyBucketName"]; !ok {
        confDebug.Panicf("Bucket name not set up yet.\n")
    } else {
        myBucketName = _myBucketName
    }
    if _ipPort, ok := config.Property["IPPort"]; !ok {
        confDebug.Panicf("P2P ip:port not set up yet.\n")
    } else {
        ipPort = _ipPort
    }
    if _daemonPort, ok := config.Property["DaemonPort"]; !ok {
        confDebug.Panicf("Port for Teapot Daemon not set up yet.\n")
    } else {
        daemonPort = _daemonPort
    }

    if aesKeyString, ok := config.Property["AESKey"]; !ok {
        confDebug.Panicf("AES key not set up yet.\n")
    } else {
        if _aesKey, err := hex.DecodeString(aesKeyString); err != nil {
            confDebug.Panicf("AES key is not in a well-formed format.\n")
        } else {
            aesKey = _aesKey
        }
    }
    if _awsAccessKey, ok := config.Property["AWSAccessKey"]; !ok {
        confDebug.Panicf("Amazon AWS access key not set up yet.\n")
    } else {
        awsAccessKey = _awsAccessKey
    }
    if _awsSecretKey, ok := config.Property["AWSSecretKey"]; !ok {
        confDebug.Panicf("Amazon AWS secret key not set up yet.\n")
    } else {
        awsSecretKey = _awsSecretKey
    }

    if encodedPrivateKey, err := base64.URLEncoding.DecodeString(config.PrivateKey); err != nil {
        confDebug.Panicf("Invalid private key.\n%+v\n", err)
    } else {
        if _privateKey, err := x509.ParsePKCS1PrivateKey(encodedPrivateKey); err != nil {
            confDebug.Panicf("Invalid private key.\n%+v\n", err)
        } else {
            privateKey = _privateKey
        }
    }
    publicKeys = make(map[string]*rsa.PublicKey)
    for nodeId, encodedPublicKey := range config.PublicKeys {
        if encodedPublicKey, err := base64.URLEncoding.DecodeString(encodedPublicKey); err != nil {
            confDebug.Panicf("Invalid public key of node %+v.\n%+v\n", nodeId, err)
        } else {
            if publicKey, err := x509.ParsePKIXPublicKey(encodedPublicKey); err != nil {
                confDebug.Panicf("Invalid public key of node %+v.\n%+v\n", nodeId, err)
            } else if publicKey, ok := publicKey.(*rsa.PublicKey); !ok {
                confDebug.Panicf("Invalid public key of node %+v.\n%+v\n", nodeId, err)
            } else {
                publicKeys[nodeId] = publicKey
            }
        }
    }
    nodeBucketMap = config.NodeBucketMap
    nodeIpMap = config.NodeIpMap
    conf := Config{
        logPath,
        journalPath,
        snapshotDir,
        valueDir,

        myNodeId,
        myBucketName,
        ipPort,
        daemonPort,

        aesKey,
        awsAccessKey,
        awsSecretKey,

        nodeBucketMap,
        nodeIpMap,
        privateKey,
        publicKeys,
    }
    return &conf, nil
}

func LoadTest(dir string, i int) *Config {
    // TODO randomize this information.
    tempDir, err := ioutil.TempDir(dir, "temp")
    if err != nil {
        panic(err.Error())
    }
    logPath := tempDir + "/log.txt"
    journalPath := tempDir + "/journal.txt"
    snapshotDir := tempDir + "test/snapshot/"
    valueDir := tempDir + "test/values/"

    myNodeId := "test_node_" + strconv.Itoa(i) + "_" + strconv.FormatInt(rand.Int63(), 10)
    myBucketName := "test_bucket_" + strconv.Itoa(i) + "_" + strconv.FormatInt(rand.Int63(), 16)
    iPPort := "127.0.0.1:" + strconv.Itoa(rand.Intn(10000)+12345)
    daemonPort := strconv.Itoa(rand.Intn(10000) + 22345)

    aESKey := utility.KeyFromPassphrase("test_key" + strconv.Itoa(rand.Int()))
    aWSAccessKey := "test_aws_access_key"
    aWSSecretKey := "test_aws_secret_key"

    nodeBucketMap := make(map[string]string)
    nodeIpMap := make(map[string]string)
    privateKey, err := rsa.GenerateKey(Reader, 1024)
    if err != nil {
        panic(err.Error())
    }
    publicKeys := make(map[string]*rsa.PublicKey)

    publicKeys[myNodeId] = &privateKey.PublicKey
    nodeBucketMap[myNodeId] = myBucketName
    nodeIpMap[myNodeId] = iPPort

    config := Config{
        logPath,
        journalPath,
        snapshotDir,
        valueDir,

        myNodeId,
        myBucketName,
        iPPort,
        daemonPort,

        aESKey,
        aWSAccessKey,
        aWSSecretKey,

        nodeBucketMap,
        nodeIpMap,
        privateKey,
        publicKeys,
    }
    return &config
}

// TODO
func LoadMultipleTest(dir string, n int) []*Config {
    configs := make([]*Config, n, n)
    for i := 0; i < n; i++ {
        configs[i] = LoadTest(dir, i)
    }
    // exchange public key and node id.
    for i := 0; i < n; i++ {
        for j := 0; j < n; j++ {
            configs[i].PublicKeys[configs[j].MyNodeId] = &configs[j].PrivateKey.PublicKey
            configs[i].NodeIpMap[configs[j].MyNodeId] = configs[j].IPPort
            configs[i].NodeBucketMap[configs[j].MyNodeId] = configs[j].MyBucketName
        }
    }
    return configs
}

func ReadConfigFile(configFilePath string) Configuration {
    configFile, err := os.Open(configFilePath)
    if err != nil {
        panic(err.Error())
    }
    defer configFile.Close()
    var config Configuration
    decoder := json.NewDecoder(configFile)
    decoder.Decode(&config)
    return config
}

func ReadNodeInfoFile(nodeInfoFilePath string) NodeInfo {
    nodeInfoFile, err := os.Open(nodeInfoFilePath)
    if err != nil {
        panic(err.Error())
    }
    defer nodeInfoFile.Close()
    var nodeInfo NodeInfo
    decoder := json.NewDecoder(nodeInfoFile)
    decoder.Decode(&nodeInfo)
    return nodeInfo
}

func WriteConfigFile(config Configuration, configFilePath string) {
    configFile, err := os.Create(configFilePath)
    if err != nil {
        panic(err.Error())
    }
    defer configFile.Close()
    configEncoder := json.NewEncoder(configFile)
    if err := configEncoder.Encode(config); err != nil {
        panic(err.Error())
    }
    configFile.Sync()
}

func WriteNodeInfoFile(nodeInfo NodeInfo, nodeInfoFilePath string) {
    pubFile, err := os.Create(nodeInfoFilePath)
    if err != nil {
        panic(err.Error())
    }
    defer pubFile.Close()
    pubEncoder := json.NewEncoder(pubFile)
    if err := pubEncoder.Encode(nodeInfo); err != nil {
        panic(err.Error())
    }
    pubFile.Sync()
}

func GenerateConfig(nodeInfo NodeInfo, passphrase, awsAccessKey, awsSecretKey, privateKey string) Configuration {
    nodeBucketMap := make(map[string]string)
    nodeIpMap := make(map[string]string)
    publicKeys := make(map[string]string)

    rand.Seed(time.Now().Unix())
    daemonPort := strconv.Itoa(rand.Intn(32768) + 32768)

    aesKey := utility.KeyFromPassphraseAndEncode(passphrase)

    nodeBucketMap[nodeInfo.NodeId] = nodeInfo.NodeBucket
    nodeIpMap[nodeInfo.NodeId] = nodeInfo.NodeIPPort
    publicKeys[nodeInfo.NodeId] = nodeInfo.PublicKey

    confDebug.Debugf("%+v", nodeInfo)
    config := Configuration{
        map[string]string{
            "LogPath":      "log.txt",
            "JournalPath":  "journal.txt",
            "SnapshotDir":  "snapshot/",
            "ValueDir":     "values/",
            "MyNodeId":     nodeInfo.NodeId,
            "MyBucketName": nodeInfo.NodeBucket,
            "IPPort":       nodeInfo.NodeIPPort,
            "DaemonPort":   daemonPort,
            "AESKey":       aesKey,
            "AWSAccessKey": awsAccessKey,
            "AWSSecretKey": awsSecretKey,
        },
        nodeBucketMap,
        nodeIpMap,
        publicKeys,
        privateKey,
    }
    return config
}

func GenerateNodeInfo(nodeId, ipPort, encodedPubKey string) NodeInfo {
    // bucketName should be generated from public key
    h := sha512.New()
    io.WriteString(h, nodeId+ipPort)
    bucketByte := h.Sum(nil)
    bucketName := hex.EncodeToString(bucketByte)

    return NodeInfo{
        nodeId,
        bucketName,
        ipPort,
        encodedPubKey,
    }
}

func AddNodeInfo(config Configuration, nodeInfo NodeInfo) Configuration {
    config.NodeBucketMap[nodeInfo.NodeId] = nodeInfo.NodeBucket
    config.NodeIpMap[nodeInfo.NodeId] = nodeInfo.NodeIPPort
    config.PublicKeys[nodeInfo.NodeId] = nodeInfo.PublicKey
    return config
}

func init() {
    rand.Seed(time.Now().Unix())
}
