package util

import (
	"sync"
)

var Handlers = map[string]func([]Value) Value{
	"PING":    ping,
	"ECHO":    echo,
	"SET":     set,
	"GET":     get,
	"HSET":    hset,
	"HGET":    hget,
	"HGETALL": hgetall,
}

func ping(args []Value) Value {
	if len(args) == 0 {
		return Value{Type: "string", Str: "PONG"}
	}
	return Value{Type: "string", Str: args[0].Bulk}
}

func echo(args []Value) Value {
	return Value{Type: "string", Str: args[0].Bulk}
}

var SETs = map[string]string{}
var SETsMu = sync.RWMutex{}

func set(args []Value) Value {
	if len(args) != 2 {
		return Value{Type: "error", Str: "ERR wrong number of arguments for 'set' command"}
	}
	key := args[0].Bulk
	value := args[1].Bulk
	SETsMu.Lock()
	SETs[key] = value
	SETsMu.Unlock()
	return Value{Type: "string", Str: "OK"}
}

func get(args []Value) Value {
	if len(args) != 1 {
		return Value{Type: "error", Str: "ERR wrong number of arguments for 'get' command"}
	}
	key := args[0].Bulk
	SETsMu.Lock()
	value, ok := SETs[key]
	SETsMu.Unlock()
	if !ok {
		return Value{Type: "null"}
	}
	return Value{Type: "bulk", Bulk: value, Num: len(value)}
}

var HSETs = map[string]map[string]string{}
var HSETsMu = sync.RWMutex{}

func hset(args []Value) Value {
	n := len(args)
	if n < 3 || n&1 == 0 {
		return Value{Type: "error", Str: "ERR wrong number of arguments for 'hset' command"}
	} else {
		hash := args[0].Bulk
		HSETsMu.Lock()
		if _, ok := HSETs[hash]; !ok {
			HSETs[hash] = map[string]string{}
		}
		iter := 1
		for i := 0; i < (n-1)/2; i++ {
			key := args[1+i*iter].Bulk
			iVal := args[2+i*iter].Bulk
			HSETs[hash][key] = iVal
			iter++
		}
		HSETsMu.Unlock()
		return Value{Type: "string", Str: "OK"}
	}
}

func hget(args []Value) Value {
	if len(args) != 2 {
		return Value{Type: "error", Str: "ERR wrong number of arguments for 'hget' command"}
	}
	hash := args[0].Bulk
	key := args[1].Bulk
	HSETsMu.Lock()
	value, ok := HSETs[hash][key]
	HSETsMu.Unlock()
	if !ok {
		return Value{Type: "null"}
	}
	return Value{Type: "bulk", Bulk: value}
}

func hgetall(args []Value) Value {
	if len(args) != 1 {
		return Value{Type: "error", Str: "ERR wrong number of arguments for 'hgetall' command"}
	}
	hash := args[0].Bulk
	HSETsMu.Lock()
	value, ok := HSETs[hash]
	HSETsMu.Unlock()
	if !ok {
		return Value{Type: "array", Num: 0}
	} else {
		ans := []Value{}
		for key, val := range value {
			ans = append(ans, Value{Type: "bulk", Num: len(key), Bulk: key})
			ans = append(ans, Value{Type: "bulk", Num: len(val), Bulk: val})
		}
		return Value{Type: "array", Num: len(value) * 2, Array: ans}
	}
}
