package streams

type StreamEntry struct {
	ID    string
	Value map[string]string
	Prev  *StreamEntry
	Next  *StreamEntry
}

type Stream struct {
	Head     *StreamEntry
	Tail     *StreamEntry
	Entries  map[string]*StreamEntry
	LastTime int64
	LastSeq  int64
	C        chan *StreamEntry
}

func NewStream() *Stream {
	return &Stream{Entries: make(map[string]*StreamEntry), LastTime: 0, LastSeq: 0, C: make(chan *StreamEntry)}
}

func (s *Stream) AddEntry(id string, value map[string]string) {
	entry := &StreamEntry{
		ID:    id,
		Value: value,
	}
	if s.Tail == nil {
		s.Head = entry
		s.Tail = entry
	} else {
		s.Tail.Next = entry
		entry.Prev = s.Tail
		s.Tail = entry
	}
	select {
	case s.C <- entry:
	default:
	}
	s.Entries[id] = entry
}

func (s *Stream) DeleteEntry(id string) {
	entry, ok := s.Entries[id]
	if !ok {
		return
	}
	if entry.Prev != nil {
		entry.Prev.Next = entry.Next
	} else {
		s.Head = entry.Next
	}
	if entry.Next != nil {
		entry.Next.Prev = entry.Prev
	} else {
		s.Tail = entry.Prev
	}
	delete(s.Entries, id)
}

func (s *Stream) RangeQuery(startID, endID string) []*StreamEntry {
	var result []*StreamEntry
	if startID == "-" {
		startID = string(rune(33))
	} else if endID == "+" {
		endID = string(rune(1114111))
	}
	for entry := s.Head; entry != nil; entry = entry.Next {
		if entry.ID >= startID && entry.ID <= endID {
			result = append(result, entry)
		}
	}
	return result
}

func (s *Stream) QueryXread(startID string) []*StreamEntry {
	var result []*StreamEntry
	for entry := s.Head; entry != nil; entry = entry.Next {
		if entry.ID > startID {
			result = append(result, entry)
		}
	}
	return result
}
