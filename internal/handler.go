package util

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	rdb "github.com/heisenberg8055/redis-rdb"
	"github.com/heisenberg8055/redis-rdb/nopdecoder"
)

type MapValue struct {
	Val string
	TTL time.Time
}

type decoder struct {
	db int
	i  int
	nopdecoder.NopDecoder
}

var Handlers = map[string]func([]Value) Value{
	"PING":    ping,
	"ECHO":    echo,
	"SET":     set,
	"GET":     get,
	"HSET":    hset,
	"HGET":    hget,
	"HGETALL": hgetall,
	"DEL":     del,
	"CONFIG":  config,
	"KEYS":    keys,
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

var mp = map[string]MapValue{}
var mpMu = sync.RWMutex{}

func set(args []Value) Value {
	n := len(args)
	switch n {
	case 1:
		return Value{Type: "error", Str: "ERR wrong number of arguments for 'set' command"}
	case 2:
		key := args[0].Bulk
		value := args[1].Bulk
		mpMu.Lock()
		mp[key] = MapValue{Val: value, TTL: time.Time{}}
		mpMu.Unlock()
	case 3:
		return Value{Type: "error", Str: "Err syntax error"}
	case 4:
		key := args[0].Bulk
		value := args[1].Bulk
		flag := strings.ToUpper(args[2].Bulk)
		ttlString := args[3].Bulk
		ttl, err := strconv.ParseInt(ttlString, 10, 64)
		if err != nil {
			return Value{Type: "error", Str: "ERR value is not an integer or out of range"}
		}
		switch flag {
		case "PX":
			mpMu.Lock()
			mp[key] = MapValue{Val: value, TTL: time.Now().Local().Add(time.Millisecond * time.Duration(ttl))}
			mpMu.Unlock()
		case "EX":
			mpMu.Lock()
			mp[key] = MapValue{Val: value, TTL: time.Now().Local().Add(time.Second * time.Duration(ttl))}
			mpMu.Unlock()
		default:
			return Value{Type: "error", Str: "Err syntax error"}
		}
	default:
		return Value{Type: "error", Str: "ERR wrong number of arguments for 'set' command"}
	}
	return Value{Type: "string", Str: "OK"}
}

func get(args []Value) Value {
	if len(args) != 1 {
		return Value{Type: "error", Str: "ERR wrong number of arguments for 'get' command"}
	}
	osArgs := os.Args
	if len(osArgs) == 5 {
		dir := os.Args[2]
		fileName := os.Args[4]
		f, err := os.Open(dir + "/" + fileName)
		if err != nil {
			return Value{Type: "error", Str: err.Error()}
		}
		err = rdb.Decode(f, &decoder{})
		if err != nil {
			return Value{Type: "error", Str: err.Error()}
		}
		mpMu.Lock()
		value, ok := mp[args[0].Bulk]
		mpMu.Unlock()
		if !ok {
			return Value{Type: "null"}
		}
		if isExpired(value.TTL) {
			delete(mp, args[0].Bulk)
			return Value{Type: "null"}
		}
		return Value{Type: "bulk", Num: len(value.Val), Bulk: value.Val}
	}
	key := args[0].Bulk
	mpMu.Lock()
	value, ok := mp[key]
	mpMu.Unlock()
	if !ok {
		return Value{Type: "null"}
	}
	if isExpired(value.TTL) {
		delete(mp, key)
		return Value{Type: "null"}
	}
	return Value{Type: "bulk", Bulk: value.Val, Num: len(value.Val)}
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

func del(args []Value) Value {
	n := len(args)
	if n == 0 {
		return Value{Type: "error", Str: "Err wrong number of arguments for 'del' command"}
	}
	deletedKeys := 0
	mpMu.Lock()
	for i := 0; i < n; i++ {
		if _, ok := mp[args[i].Bulk]; ok {
			deletedKeys++
			delete(mp, args[i].Bulk)
		}
	}
	mpMu.Unlock()
	return Value{Type: "integer", Num: deletedKeys}
}

func isExpired(t time.Time) (expired bool) {
	if t.IsZero() {
		return false
	}
	return time.Now().After(t)
}

func config(args []Value) Value {
	n := len(args)
	switch n {
	case 0:
		return Value{Type: "error", Str: "ERR wrong number of arguments for 'config' command"}
	case 1:
		subCommand := strings.ToUpper(args[0].Bulk)
		if subCommand == "GET" {
			return Value{Type: "error", Str: "ERR wrong number of arguments for 'config|get' command"}
		} else {
			return Value{Type: "error", Str: fmt.Sprintf("ERR unknown subcommand '%s'.", subCommand)}
		}
	case 2:
		subCommand := strings.ToUpper(args[0].Bulk)
		if subCommand == "GET" {
			param := args[1].Bulk
			ans := []Value{}
			switch param {
			case "dir":
				val := os.Args[2]
				ans = append(ans, Value{Type: "bulk", Bulk: "dir", Num: len("dir")})
				ans = append(ans, Value{Type: "bulk", Bulk: val, Num: len(val)})
				return Value{Type: "array", Num: 2, Array: ans}
			case "dbfilename":
				val := os.Args[4]
				ans = append(ans, Value{Type: "bulk", Bulk: "dbfileName", Num: len("dbfileName")})
				ans = append(ans, Value{Type: "bulk", Bulk: val, Num: len(val)})
				return Value{Type: "array", Num: 2, Array: ans}
			}
		} else {
			return Value{Type: "error", Str: fmt.Sprintf("ERR unknown subcommand '%s'.", args[0].Bulk)}
		}
	}
	return Value{}
}

func (p *decoder) Set(key, value []byte, expiry int64) {
	if expiry != 0 {
		mpMu.Lock()
		mp[string(key)] = MapValue{Val: string(value), TTL: time.Unix(expiry/1000, 0)}
		mpMu.Unlock()
		return
	}
	mpMu.Lock()
	mp[string(key)] = MapValue{Val: string(value), TTL: time.Time{}}
	mpMu.Unlock()
}

func keys(args []Value) Value {
	n := len(args)
	if n != 1 {
		return Value{Type: "error", Str: "ERR wrong number of arguments for 'keys' command"}
	}
	dir := os.Args[2]
	fileName := os.Args[4]
	f, err := os.Open(dir + "/" + fileName)
	if err != nil {
		return Value{Type: "error", Str: err.Error()}
	}
	err = rdb.Decode(f, &decoder{})
	if err != nil {
		return Value{Type: "error", Str: err.Error()}
	}
	ans := []Value{}
	mpMu.Lock()
	for k, v := range mp {
		if isExpired(v.TTL) {
			delete(mp, k)
			continue
		}
		ans = append(ans, Value{Type: "bulk", Bulk: k, Num: len(k)})
	}
	mpMu.Unlock()
	return Value{Type: "array", Array: ans, Num: len(ans)}
}
