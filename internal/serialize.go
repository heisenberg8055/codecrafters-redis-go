package util

import (
	"io"
	"strconv"
)

type Writer struct {
	writer io.Writer
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{writer: w}
}

func (w *Writer) Write(v Value) error {
	var bytes = v.Marshall()
	_, err := w.writer.Write(bytes)
	if err != nil {
		return err
	}
	return nil
}

func (v Value) Marshall() []byte {
	switch v.Type {
	case "array":
		return v.marshallArray()
	case "bulk":
		return v.marshallBulk()
	case "string":
		return v.marshallString()
	case "null":
		return v.marshallNull()
	case "error":
		return v.marshallError()
	default:
		return []byte{}
	}
}

func (v *Value) marshallString() []byte {
	var bytes []byte
	bytes = append(bytes, STRING)
	bytes = append(bytes, v.Str...)
	bytes = append(bytes, CARRIAGERETURN, LINEFEED)
	return bytes
}

func (v *Value) marshallBulk() []byte {
	var bytes []byte
	bytes = append(bytes, BULK)
	bytes = append(bytes, strconv.Itoa(v.Num)...)
	bytes = append(bytes, CARRIAGERETURN, LINEFEED)
	bytes = append(bytes, v.Bulk...)
	bytes = append(bytes, CARRIAGERETURN, LINEFEED)
	return bytes
}

func (v *Value) marshallArray() []byte {
	len := v.Num
	var bytes []byte

	bytes = append(bytes, ARRAY)
	bytes = append(bytes, strconv.Itoa(len)...)
	bytes = append(bytes, CARRIAGERETURN, LINEFEED)
	for i := 0; i < len; i++ {
		bytes = append(bytes, v.Array[i].Marshall()...)
	}
	return bytes
}

func (v *Value) marshallError() []byte {
	var bytes []byte
	bytes = append(bytes, ERROR)
	bytes = append(bytes, v.Str...)
	bytes = append(bytes, CARRIAGERETURN, LINEFEED)
	return bytes
}

func (v *Value) marshallNull() []byte {
	return []byte("$-1\r\n")
}
