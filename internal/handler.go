package util

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/codecrafters-io/redis-starter-go/internal/streams"
	rdb "github.com/heisenberg8055/redis-rdb"
	"github.com/heisenberg8055/redis-rdb/nopdecoder"
)

type RedisMapValue struct {
	Val     string
	TTL     time.Time
	Keytype string
	Stream  *streams.Stream
}

type decoder struct {
	nopdecoder.NopDecoder
}

type Transaction struct {
	IsMulti bool
	Execs   []map[string][]Value
}

var transaction Transaction

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
	"TYPE":    types,
	"XADD":    xadd,
	"XRANGE":  xrange,
	"XREAD":   xread,
	"INCR":    incr,
	"MULTI":   multi,
	"EXEC":    exec,
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

var mp = map[string]RedisMapValue{}
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
		mp[key] = RedisMapValue{Val: value, TTL: time.Time{}, Keytype: "string"}
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
			mp[key] = RedisMapValue{Val: value, TTL: time.Now().Local().Add(time.Millisecond * time.Duration(ttl)), Keytype: "string"}
			mpMu.Unlock()
		case "EX":
			mpMu.Lock()
			mp[key] = RedisMapValue{Val: value, TTL: time.Now().Local().Add(time.Second * time.Duration(ttl)), Keytype: "string"}
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
	return Value{Type: "integer", Str: strconv.Itoa(deletedKeys)}
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
		mp[string(key)] = RedisMapValue{Val: string(value), TTL: time.Unix(expiry/1000, 0), Keytype: "string"}
		mpMu.Unlock()
		return
	}
	mpMu.Lock()
	mp[string(key)] = RedisMapValue{Val: string(value), TTL: time.Time{}, Keytype: "string"}
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

func types(args []Value) Value {
	n := len(args)
	if n == 0 {
		return Value{Type: "error", Str: "Err wrong number of arguments for 'type' command"}
	}
	key := args[0].Bulk
	mpMu.Lock()
	value, ok := mp[key]
	mpMu.Unlock()
	if !ok {
		return Value{Type: "string", Str: "none"}
	}
	return Value{Type: "string", Str: value.Keytype}
}

func xadd(args []Value) Value {
	n := len(args)
	if n == 0 || n%2 == 1 {
		return Value{Type: "error", Str: "ERR wrong number of arguments for 'xadd' command"}
	}
	streamName := args[0].Bulk
	streamID := args[1].Bulk
	streamIdArray := strings.Split(streamID, "-")
	mpMu.Lock()
	value, ok := mp[streamName]
	mpMu.Unlock()
	mapVal := make(map[string]string)
	for i := 2; i < n; i += 2 {
		k := args[i].Bulk
		v := args[i+1].Bulk
		mapVal[k] = v
	}
	switch {
	case streamID == "0-0":
		return Value{Type: "error", Str: "ERR The ID specified in XADD must be greater than 0-0"}
	case streamID == "*":
		if !ok {
			timeUnix := time.Now().Unix() * 1000
			newStream := streams.NewStream()
			newId := fmt.Sprintf("%d-0", timeUnix)
			newStream.AddEntry(newId, mapVal)
			mpMu.Lock()
			mp[streamName] = RedisMapValue{Stream: newStream, TTL: time.Time{}, Keytype: "stream"}
			mpMu.Unlock()
			return Value{Type: "bulk", Bulk: newId, Num: len(newId)}
		} else {
			prevSeq := value.Stream.LastSeq
			prevTime := value.Stream.LastTime
			prevId := value.Stream.Tail.ID
			timeUnix := time.Now().Unix() * 1000
			if prevId > fmt.Sprintf("%d-0", timeUnix) {
				return Value{Type: "error", Str: "ERR The ID specified in XADD is equal or smaller than the target stream top item"}
			}
			if timeUnix == prevTime {
				newId := fmt.Sprintf("%d-%d", timeUnix, prevSeq+1)
				value.Stream.AddEntry(newId, mapVal)
				return Value{Type: "bulk", Num: len(newId), Bulk: newId}
			} else {
				newId := fmt.Sprintf("%d-%d", timeUnix, 0)
				value.Stream.AddEntry(newId, mapVal)
				return Value{Type: "bulk", Num: len(newId), Bulk: newId}
			}
		}
	case streamIdArray[len(streamIdArray)-1] == "*":
		if !ok {
			newStream := streams.NewStream()
			switch {
			case streamID == "0-*":
				newStream.AddEntry("0-1", mapVal)
				mpMu.Lock()
				mp[streamName] = RedisMapValue{TTL: time.Time{}, Keytype: "stream", Stream: newStream}
				mpMu.Unlock()
				return Value{Type: "bulk", Num: 3, Bulk: "0-1"}
			default:
				newId := streamIdArray[0] + "-0"
				newStream.AddEntry(newId, mapVal)
				mpMu.Lock()
				mp[streamName] = RedisMapValue{TTL: time.Time{}, Keytype: "stream", Stream: newStream}
				mpMu.Unlock()
				return Value{Type: "bulk", Num: len(newId), Bulk: newId}
			}
		} else {
			prevId := value.Stream.Tail.ID
			prevIdArr := strings.Split(prevId, "-")
			if prevIdArr[0] > streamIdArray[0] {
				return Value{Type: "error", Str: "ERR The ID specified in XADD is equal or smaller than the target stream top item"}
			}
			prevIdSeq, _ := strconv.Atoi(prevIdArr[1])
			if prevIdArr[0] == streamIdArray[0] {
				nextID := streamIdArray[0] + fmt.Sprintf("-%d", prevIdSeq+1)
				value.Stream.AddEntry(nextID, mapVal)
				return Value{Type: "bulk", Num: len(nextID), Bulk: nextID}
			} else {
				nextID := streamIdArray[0] + "-0"
				value.Stream.AddEntry(nextID, mapVal)
				return Value{Type: "bulk", Num: len(nextID), Bulk: nextID}
			}
		}
	default:
		if !ok {
			newStream := streams.NewStream()
			newStream.AddEntry(streamID, mapVal)
			mpMu.Lock()
			mp[streamName] = RedisMapValue{TTL: time.Time{}, Keytype: "stream", Stream: newStream}
			mpMu.Unlock()
		} else {
			if value.Stream.Tail.ID >= streamID {
				return Value{Type: "error", Str: "ERR The ID specified in XADD is equal or smaller than the target stream top item"}
			}
			value.Stream.AddEntry(streamID, mapVal)
		}
		return Value{Type: "bulk", Num: len(streamID), Bulk: streamID}
	}
}

func xrange(args []Value) Value {
	n := len(args)
	if n != 3 {
		return Value{Type: "error", Str: "ERR wrong number of arguments for 'xrange' command"}
	}
	key := args[0].Bulk
	startIndex := args[1].Bulk
	endIndex := args[2].Bulk
	mpMu.Lock()
	value, ok := mp[key]
	mpMu.Unlock()
	if !ok {
		return Value{Type: "array", Num: 0, Array: []Value{}}
	}
	entries := value.Stream.RangeQuery(startIndex, endIndex)
	entryArr := []Value{}
	for _, entry := range entries {
		arr := []Value{}
		arr = append(arr, Value{Type: "bulk", Num: len(entry.ID), Bulk: entry.ID})
		values := []Value{}
		for k, v := range entry.Value {
			values = append(values, Value{Type: "bulk", Num: len(k), Bulk: k})
			values = append(values, Value{Type: "bulk", Num: len(v), Bulk: v})
		}
		arr = append(arr, Value{Type: "array", Num: len(values), Array: values})
		entryArr = append(entryArr, Value{Type: "array", Num: len(arr), Array: arr})
	}
	return Value{Type: "array", Num: len(entryArr), Array: entryArr}
}

func xread(args []Value) Value {
	n := len(args)
	if n < 3 || n&1 == 0 {
		return Value{Type: "error", Str: "ERR wrong number of arguments for 'xread' command"}
	}
	if args[0].Bulk != "streams" && args[0].Bulk != "block" {
		return Value{Type: "error", Str: "Err systax error"}
	}
	switch {
	case args[0].Bulk == "block":
		t, err := strconv.ParseInt(args[1].Bulk, 10, 64)
		if err != nil {
			return Value{Type: "error", Str: "Err invalid block time"}
		}
		key := args[3].Bulk
		var entry *streams.StreamEntry
		timer := time.NewTimer(time.Duration(t) * time.Millisecond)
		if t == 0 {
			timer = time.NewTimer(time.Hour * 24 * 365)
		}
		defer timer.Stop()
		stream, ok := mp[key]
		if !ok {
			return Value{Type: "null"}
		}
		channel := stream.Stream.C
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			select {
			case <-timer.C:
				wg.Done()
				return
			case entri := <-channel:
				entry = entri
				wg.Done()
				return
			}
		}()
		wg.Wait()
		if entry == nil {
			return Value{Type: "null"}
		}
		ans := []Value{}
		streamArr := []Value{}
		entryArr := []Value{}
		arr := []Value{}
		arr = append(arr, Value{Type: "bulk", Num: len(entry.ID), Bulk: entry.ID})
		values := []Value{}
		for k, v := range entry.Value {
			values = append(values, Value{Type: "bulk", Num: len(k), Bulk: k})
			values = append(values, Value{Type: "bulk", Num: len(v), Bulk: v})
		}
		arr = append(arr, Value{Type: "array", Num: len(values), Array: values})
		entryArr = append(entryArr, Value{Type: "array", Num: len(arr), Array: arr})
		streamArr = append(streamArr, Value{Type: "bulk", Num: len(key), Bulk: key})
		streamArr = append(streamArr, Value{Type: "array", Num: len(entryArr), Array: entryArr})
		ans = append(ans, Value{Type: "array", Num: len(streamArr), Array: streamArr})
		return Value{Type: "array", Num: len(ans), Array: ans}
	case args[0].Bulk == "streams":
		ans := []Value{}
		for i := 1; i <= n/2; i++ {
			streamArr := []Value{}
			key := args[i].Bulk
			id := args[(n/2)+i].Bulk
			mpMu.Lock()
			value := mp[key]
			mpMu.Unlock()
			entryArr := []Value{}
			entries := value.Stream.QueryXread(id)
			for _, entry := range entries {
				arr := []Value{}
				arr = append(arr, Value{Type: "bulk", Num: len(entry.ID), Bulk: entry.ID})
				values := []Value{}
				for k, v := range entry.Value {
					values = append(values, Value{Type: "bulk", Num: len(k), Bulk: k})
					values = append(values, Value{Type: "bulk", Num: len(v), Bulk: v})
				}
				arr = append(arr, Value{Type: "array", Num: len(values), Array: values})
				entryArr = append(entryArr, Value{Type: "array", Num: len(arr), Array: arr})
			}
			streamArr = append(streamArr, Value{Type: "bulk", Num: len(key), Bulk: key})
			streamArr = append(streamArr, Value{Type: "array", Num: len(entryArr), Array: entryArr})
			ans = append(ans, Value{Type: "array", Num: len(streamArr), Array: streamArr})
		}
		return Value{Type: "array", Num: len(ans), Array: ans}
	}
	return Value{Type: "error", Str: "Err"}
}

func incr(args []Value) Value {
	key := args[0].Bulk
	mpMu.Lock()
	value, ok := mp[key]
	mpMu.Unlock()
	if !ok {
		mp[key] = RedisMapValue{Keytype: "string", Val: "1"}
		return Value{Type: "integer", Str: "1"}
	}
	updateNum := value.Val
	updateCast, err := strconv.Atoi(updateNum)
	if err != nil {
		return Value{Type: "error", Str: "ERR value is not an integer or out of range"}
	}
	ans := strconv.Itoa(updateCast + 1)
	mp[key] = RedisMapValue{Keytype: "string", Val: ans}
	return Value{Type: "integer", Str: ans}
}

func multi(args []Value) Value {
	if len(args) != 0 {
		return Value{Type: "error", Str: "ERR wrong number of arguments for 'multi' command"}
	}
	if !transaction.IsMulti {
		transaction.IsMulti = true
		return Value{Type: "string", Str: "OK"}
	}
	return Value{Type: "error", Str: "ERR MULTI calls can not be nested"}
}

func exec(args []Value) Value {
	if len(args) != 0 {
		return Value{Type: "error", Str: "Err"}
	}
	if !transaction.IsMulti {
		return Value{Type: "error", Str: "ERR EXEC without MULTI"}
	}
	arr := transaction.Execs
	if len(arr) == 0 {
		transaction.IsMulti = false
		return Value{Type: "array", Num: 0, Array: []Value{}}
	}
	return Value{}
}
