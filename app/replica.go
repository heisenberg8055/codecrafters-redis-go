package main

import (
	"fmt"
	"net"
	"os"
)

func NewReplica(port string) {
	fmt.Println(port)
	l, err := net.Listen("tcp", ":"+port)
	if err != nil {
		fmt.Printf("Failed to create replica on port: %v", port)
		os.Exit(1)
	}
	defer l.Close()
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Printf("Error accepting connection: %v", err.Error())
			os.Exit(1)
		}
		go handleConnection(conn)
	}
}
