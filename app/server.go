package main

import (
	"fmt"
	"net"
	"os"
	"sync"
)

var wg sync.WaitGroup

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")
	// Uncomment this block to pass the first stage
	//
	l, err := net.Listen("tcp", "127.0.0.1:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer l.Close()
	conn, err := l.Accept()
	for {
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		wg.Add(1)
		go handleConnection(conn, &wg)
		wg.Wait()
	}
}
func handleConnection(conn net.Conn, wg *sync.WaitGroup) {
	buf := make([]byte, 1024)
	conn.Read(buf)
	_, err := conn.Write([]byte("+PONG\r\n"))
	if err != nil {
		fmt.Println("Error writing connection: ", err.Error())
		os.Exit(1)
	}
	wg.Done()
}
