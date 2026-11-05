package main

import (
    "flag"
    "fmt"
	"bufio"
	"os"
)

func main() {
    id := flag.Int("id", 1, "Node ID")
    flag.Parse()

    node := NewNode(*id)

    go func() {
        err := node.StartServer()
        if err != nil {
            fmt.Println("Server error:", err)
        }
    }()

	err := node.ConnectToPeers()
	if err != nil {
    	fmt.Println("Failed to connect to peers:", err)
    	return
	}

	scanner := bufio.NewScanner(os.Stdin)

	fmt.Println("Node", *id, "is running. Type 'request' to enter the critical section.")

	for scanner.Scan() {
    	command := scanner.Text()

    	if command == "request" {
        	go node.RequestCS()
    	}
	}
}