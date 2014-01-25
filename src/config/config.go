package main

import (
    "flag"
    "fmt"
    "io/ioutil"
    "launchpad.net/goamz/aws"
    "os"
    "regexp"
    "teapot/conf"
    "teapot/utility"
)

func Usage() {
    fmt.Println("config generate <nodeId> <passphrase> <ip:port>")
    fmt.Println("config add teapot.config <nodeinfo>")
}

func checkNodeId(nodeId string) bool {
    if matched, err := regexp.MatchString("^[[:alnum:]]+$", nodeId); err == nil {
        return matched
    }
    return false
}

func checkPassphrase(passphrase string) bool {
    if matched, err := regexp.MatchString("^[[:graph:]]+$", passphrase); err == nil {
        return matched
    }
    return false
}

func checkIpAddress(ipPort string) bool {
    if matched, err := regexp.MatchString("^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?):(?:[0-9]{1,5})$", ipPort); err == nil {
        return matched
    }
    return false
}

func generate() {
    if flag.NArg() != 4 {
        Usage()
        return
    }
    nodeId := flag.Arg(1)
    passphrase := flag.Arg(2)
    ipPort := flag.Arg(3)

    // sanity check of validity of user's input.
    if !checkNodeId(nodeId) {
        fmt.Println("Invalid node ID. Node ID should only contain letters and digits.")
        os.Exit(1)
    }
    if !checkPassphrase(passphrase) {
        fmt.Println("Invalid passphrase.")
        os.Exit(1)
    }
    if !checkIpAddress(ipPort) {
        fmt.Println("Invalid IP address.")
        os.Exit(1)
    }

    auth, err := aws.EnvAuth()
    if err != nil {
        auth.AccessKey = "AKIAJQ65GFTNDA5WQROA"
        auth.SecretKey = "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"
    }
    encodedPriKey, encodedPubKey := utility.GenerateKeyPairAndEncode()
    nodeInfo := conf.GenerateNodeInfo(nodeId, ipPort, encodedPubKey)
    config := conf.GenerateConfig(nodeInfo, passphrase, auth.AccessKey, auth.SecretKey, encodedPriKey)
    conf.WriteConfigFile(config, "teapot.config")
    conf.WriteNodeInfoFile(nodeInfo, "teapot.pub."+nodeId)
    fmt.Printf("Configuration generated.\nOutput files are teapot.config and teapot.pub.%s.\n", nodeId)
}

func addNode() {
    if flag.NArg() != 3 {
        Usage()
        return
    }
    configFilePath := flag.Arg(1)
    nodeInfoFilePath := flag.Arg(2)

    config := conf.ReadConfigFile(configFilePath)
    nodeInfo := conf.ReadNodeInfoFile(nodeInfoFilePath)
    config = conf.AddNodeInfo(config, nodeInfo)

    newConfigFile, err := ioutil.TempFile("./", configFilePath)
    if err != nil {
        fmt.Println(err.Error())
        os.Exit(1)
    }
    newConfigFile.Close()
    conf.WriteConfigFile(config, newConfigFile.Name())
    if err := os.Rename(newConfigFile.Name(), configFilePath); err != nil {
        fmt.Println(err.Error())
        os.Exit(1)
    }
    fmt.Printf("%s added to teapot.config.\n", nodeInfoFilePath)
}

func main() {
    flag.Parse()
    op := flag.Arg(0)
    switch op {
    case "generate":
        generate()
    case "add":
        addNode()
    default:
        Usage()
    }
}
