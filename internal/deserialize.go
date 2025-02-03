package util

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"time"
)

const (
	STRING         = '+'
	ERROR          = '-'
	INTEGER        = ':'
	BULK           = '$'
	ARRAY          = '*'
	NULL           = '_'
	BOOLEAN        = '#'
	DOUBLE         = ','
	BIGNUM         = '('
	BULKERROR      = '!'
	MAP            = '%'
	SET            = '~'
	CARRIAGERETURN = '\r'
	LINEFEED       = '\n'
)

type Value struct {
	Type  string  // Datatype
	Str   string  // Simple String
	Num   int     // Integer
	Bulk  string  // Bulk String
	Array []Value // Arrays
	TTL   time.Time
}

type Resp struct {
	reader *bufio.Reader
}

func NewResp(rd io.Reader) *Resp {
	return &Resp{reader: bufio.NewReader(rd)}
}

func (r *Resp) readLine() (line []byte, n int, err error) {
	for {
		b, err := r.reader.ReadByte()
		if err != nil {
			return nil, 0, err
		}
		n += 1
		line = append(line, b)
		if len(line) >= 2 && line[len(line)-2] == '\r' {
			break
		}
	}
	return line[:len(line)-2], n, nil
}

func (r *Resp) readInteger() (x int, n int, err error) {
	line, n, err := r.readLine()
	if err != nil {
		return 0, 0, err
	}
	size, err := strconv.Atoi(string(line))
	if err != nil {
		return 0, 0, err
	}
	return size, n, nil
}

func (r *Resp) Read() (Value, error) {
	_type, err := r.reader.ReadByte()
	if err != nil {
		return Value{}, err
	}

	switch _type {
	case ARRAY:
		return r.readArray()
	case BULK:
		return r.readBulk()
	default:
		fmt.Printf("Unknown type: %v", string(_type))
		return Value{}, nil
	}
}

func (r *Resp) readArray() (Value, error) {
	v := Value{}
	v.Type = "array"
	length, _, err := r.readInteger()
	if err != nil {
		return v, err
	}
	v.Num = length
	v.Array = make([]Value, length)
	for i := 0; i < length; i++ {
		val, err := r.Read()
		if err != nil {
			return v, err
		}
		v.Array[i] = val
	}
	return v, nil
}

func (r *Resp) readBulk() (Value, error) {
	v := Value{}
	v.Type = "bulk"
	len, _, err := r.readInteger()
	if err != nil {
		return v, err
	}
	bulk := make([]byte, len)
	r.reader.Read(bulk)
	v.Num = len
	v.Bulk = string(bulk)
	r.readLine()
	return v, nil
}
