package main

import (
	"fmt"
	"net"
	"os"
	"strings"

	util "github.com/codecrafters-io/redis-starter-go/internal"
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")
	// Uncomment this block to pass the first stage
	//
	l, err := net.Listen("tcp", ":6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer l.Close()
	aof, err := util.NewAof("database.aof")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer aof.Close()
	aof.Read(func(value util.Value) {
		command := strings.ToUpper(value.Array[0].Bulk)
		args := value.Array[1:]

		handler, ok := util.Handlers[command]
		if !ok {
			fmt.Println("Invalid command: ", command)
			return
		}

		handler(args)
	})
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handleConnection(conn, aof)
	}
}
func handleConnection(conn net.Conn, aof *util.Aof) {
	for {
		resp := util.NewResp(conn)
		value, err := resp.Read()
		if err != nil {
			fmt.Println(err)
			return
		}
		if value.Type != "array" {
			fmt.Println("Invalid Request, expected array")
			continue
		}
		command := strings.ToUpper(value.Array[0].Bulk)
		args := value.Array[1:]
		if len(value.Array) == 0 {
			fmt.Println("Invalid Request, expected array with length > 0")
		}
		writer := util.NewWriter(conn)
		handlers, ok := util.Handlers[command]
		if !ok {
			fmt.Println("Invalid Command: ", command)
			writer.Write(util.Value{Type: "string", Str: ""})
			continue
		}
		if command == "SET" || command == "HSET" {
			aof.Write(value)
		}
		result := handlers(args)
		writer.Write(result)
	}
}
