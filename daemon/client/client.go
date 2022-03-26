package client

import (
	"log"
	"net"
	"net/rpc"
	"os/exec"
	//"strings"

	"github.com/rounakdatta/GoDFS/client"
	"github.com/rounakdatta/GoDFS/util"
)

func PutHandler(nameNodeAddress string, sourcePath string, fileName string) bool {
	rpcClient, err := initializeClientUtil(nameNodeAddress)
	util.Check(err)
	defer rpcClient.Close()
	return client.Put(rpcClient, sourcePath, fileName)
}

func GetHandler(nameNodeAddress string, fileName string) (string, bool) {
	rpcClient, err := initializeClientUtil(nameNodeAddress)
	util.Check(err)
	defer rpcClient.Close()
	return client.Get(rpcClient, fileName)
}

func PsHandler(sortingFilter string) (string, bool) {
	if sortingFilter == "ram" {
		sortingFilter = "%mem"
	}

	var cmd *exec.Cmd
	if sortingFilter != "" {
		cmd = exec.Command("ps", "aux", "--sort", sortingFilter)
	} else {
		cmd = exec.Command("ps", "aux")
	}

	message, err := cmd.Output()

	util.Check(err)
	
	if err == nil {
		return string(message), true
	} else {
		return "", false
	}
}

func initializeClientUtil(nameNodeAddress string) (*rpc.Client, error) {
	host, port, err := net.SplitHostPort(nameNodeAddress)
	util.Check(err)

	log.Printf("NameNode to connect to is %s\n", nameNodeAddress)
	return rpc.Dial("tcp", host+":"+port)
}
