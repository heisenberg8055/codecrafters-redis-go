package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	util "github.com/codecrafters-io/redis-starter-go/internal"
)

type Action struct {
	command string
	args    []util.Value
}

type Transaction struct {
	IsMulti bool
	Execs   []Action
}

var slaves map[net.Conn]bool = make(map[net.Conn]bool)

var MasterBuffer []util.Value

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")
	// Uncomment this block to pass the first stage
	//

	port := flag.String("port", "6379", "port of server")
	replicaof := flag.String("replicaof", "", "port of master server")
	_ = flag.String("dir", "", "directory of redis rdb")
	_ = flag.String("dbfilename", "", "filename of rdb file")
	flag.Parse()
	replicaOfArr := strings.Split(*replicaof, " ")
	if len(replicaOfArr) > 1 {
		go func() {
			server := fmt.Sprintf("%v:%v", replicaOfArr[0], replicaOfArr[1])
			connClient, err := net.Dial("tcp", server)
			if err != nil {
				fmt.Println(err.Error())
				os.Exit(1)
			}
			connClient.Write([]byte("*1\r\n$4\r\nPING\r\n"))
			time.Sleep(time.Second * 1)
			connClient.Write([]byte(fmt.Sprintf("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n%v\r\n", *port)))
			time.Sleep(time.Second * 1)
			connClient.Write([]byte("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"))
			time.Sleep(time.Second * 1)
			connClient.Write([]byte("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"))
		}()
	}
	l, err := net.Listen("tcp", fmt.Sprintf(":%v", *port))
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer l.Close()
	// aof, err := util.NewAof("database.aof")
	// if err != nil {
	// 	fmt.Println(err)
	// 	os.Exit(1)
	// }
	// defer aof.Close()
	// aof.Read(func(value util.Value) {
	// 	command := strings.ToUpper(value.Array[0].Bulk)
	// 	args := value.Array[1:]

	// 	handler, ok := util.Handlers[command]
	// 	if !ok {
	// 		fmt.Println("Invalid command: ", command)
	// 		return
	// 	}

	// 	handler(args)
	// })
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handleConnection(conn)
	}
}
func handleConnection(conn net.Conn) {
	var transaction Transaction = Transaction{IsMulti: false, Execs: []Action{}}
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
		if command == "SET" {
			MasterBuffer = append(MasterBuffer, value)
		}
		writer := util.NewWriter(conn)
		if command == "MULTI" {
			res := multi(args, &transaction)
			writer.Write(res)
			continue
		} else if command == "EXEC" {
			res := exec(args, &transaction)
			writer.Write(res)
			continue
		} else if command == "DISCARD" {
			res := discard(args, &transaction)
			writer.Write(res)
			continue
		}
		handlers, ok := util.Handlers[command]
		if !ok {
			fmt.Println("Invalid Command: ", command)
			writer.Write(util.Value{Type: "string", Str: ""})
			continue
		}
		if transaction.IsMulti {
			transaction.Execs = append(transaction.Execs, Action{command: command, args: args})
			res := util.Value{Type: "string", Str: "QUEUED"}
			writer.Write(res)
			continue
		}
		result := handlers(args)
		writer.Write(result)
		if command == "REPLCONF" {
			_, ok := slaves[conn]
			if !ok {
				slaves[conn] = true
			}
		}
		if isPropagationCommand(command) {
			go replicate()
		}
	}
}

func multi(args []util.Value, transaction *Transaction) util.Value {
	if len(args) != 0 {
		return util.Value{Type: "error", Str: "ERR wrong number of arguments for 'multi' command"}
	}
	if !transaction.IsMulti {
		transaction.IsMulti = true
		return util.Value{Type: "string", Str: "OK"}
	}
	return util.Value{Type: "error", Str: "ERR MULTI calls can not be nested"}
}

func exec(args []util.Value, transaction *Transaction) util.Value {
	if len(args) != 0 {
		return util.Value{Type: "error", Str: "Err"}
	}
	if !transaction.IsMulti {
		return util.Value{Type: "error", Str: "ERR EXEC without MULTI"}
	}
	queue := transaction.Execs
	if len(queue) == 0 {
		transaction.IsMulti = false
		return util.Value{Type: "array", Num: 0, Array: []util.Value{}}
	}
	output := []util.Value{}
	for _, iter := range queue {
		command := iter.command
		args := iter.args
		handlers := util.Handlers[command]
		output = append(output, handlers(args))
	}
	transaction.IsMulti = false
	transaction.Execs = []Action{}
	return util.Value{Type: "array", Num: len(output), Array: output}
}

func discard(args []util.Value, transaction *Transaction) util.Value {
	if len(args) != 0 {
		return util.Value{Type: "error", Str: "ERR wrong number of arguments for 'discard' command"}
	}
	if transaction.IsMulti {
		transaction.IsMulti = false
		transaction.Execs = []Action{}
		return util.Value{Type: "string", Str: "OK"}
	}
	return util.Value{Type: "error", Str: "ERR DISCARD without MULTI"}
}

func isPropagationCommand(command string) bool {
	return command == "SET" || command == "DEL"
}

func replicate() {
	for conn := range slaves {
		for _, val := range MasterBuffer {

			_, err := conn.Write(val.Marshall())
			if err != nil {
				fmt.Println(err.Error())
			}
		}
	}
	MasterBuffer = []util.Value{}
}
