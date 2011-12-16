package main

import (
	"os"
	"net"
	"bufio"
	"strings"
	"strconv"
	"regexp"
	"time"
	"fmt"
	"storage"
)

var spaceMatcher, _ = regexp.Compile("  *")
var mainStorage storage.NotifyStorage

type Session struct {
	conn      *net.TCPConn
	bufreader *bufio.Reader
}

type Command interface {
	Exec(s *Session) os.Error
}

type StorageCommand struct {
	command    string
	key        string
	flags      uint32
	exptime    uint32
	bytes      uint32
	cas_unique uint64
	noreply    bool
	data       []byte
}

type RetrievalCommand struct {
	command string
	keys    []string
}

const (
	NA = iota
	UnkownCommand
	ClientError
	ServerError
)

const secondsInMonth = 60 * 60 * 24 * 30

type ErrCommand struct {
	errtype int
	errdesc string
	os_err  os.Error
}

func NewSession(conn *net.TCPConn) *Session {
	s := &Session{}
	s.Init(conn)
	return s
}

func (s *Session) Init(conn *net.TCPConn) os.Error {
	s.conn = conn
	s.bufreader = bufio.NewReader(conn)
	logger.Println("New session from ", conn.RemoteAddr().String())
	return nil
}

func (s *Session) NextCommand() (Command, os.Error) {
	var line []string
	if rawline, _, err := s.bufreader.ReadLine(); err != nil {
		return nil, err
	} else {
		line = strings.Split(spaceMatcher.ReplaceAllString(string(rawline), " "), " ")
	}

	switch line[0] {
	case "set", "add", "replace", "append", "prepend", "cas":
		command := new(StorageCommand)
		if err := command.parse(line); err != nil {
			return &ErrCommand{ClientError, "bad command line format", err}, nil
		} else if err := command.readData(s.bufreader); err != nil {
			return &ErrCommand{ClientError, "bad data chunk", err}, nil
		}
		return command, nil

	case "get", "gets":
		command := new(RetrievalCommand)
		if err := command.parse(line); err != nil {
			return &ErrCommand{ClientError, "bad command line format", err}, nil
		}
		return command, nil
	case "delete":
	case "incr", "decr":
	case "touch":
	case "stats":
	case "flush_all":
	case "version":
	case "quit":
	}

	return &ErrCommand{UnkownCommand, "",
		os.NewError("Unkown command: " + line[0])}, nil
}

////////////////////////////// ERROR COMMANDS //////////////////////////////

func (e *ErrCommand) Exec(s *Session) os.Error {
	var msg string
	switch e.errtype {
	case UnkownCommand:
		msg = "ERROR\r\n"
	case ClientError:
		msg = "CLIENT_ERROR " + e.errdesc + "\r\n"
	case ServerError:
		msg = "SERVER_ERROR " + e.errdesc + "\r\n"
	}
	if e.os_err != nil {
		logger.Println(e.os_err)
	}
	if _, err := s.conn.Write([]byte(msg)); err != nil {
		return err
	}
	return nil
}

///////////////////////////// RETRIEVAL COMMANDS ////////////////////////////


func (sc *RetrievalCommand) parse(line []string) os.Error {
	if len(line) < 2 {
		return os.NewError("Bad retrieval command: missing parameters")
	}
	sc.command = line[0]
	sc.keys = line[1:]
	return nil
}

func (self *RetrievalCommand) Exec(s *Session) os.Error {

	logger.Printf("Retrieval: command: %s, keys: %s", self.command, self.keys)
	showAll := self.command == "gets"
	for i := 0; i < len(self.keys); i++ {
		if flags, bytes, cas_unique, content, err := mainStorage.Get(self.keys[i]); err != nil {
			//Internal error. Just close the connection
			s.conn.Close()
		} else if content != nil {
			if showAll {
				s.conn.Write([]byte(fmt.Sprintf("VALUE %s %d %d %d\r\n", self.keys[i], flags, bytes, cas_unique)))
			} else {
				s.conn.Write([]byte(fmt.Sprintf("VALUE %s %d %d\r\n", self.keys[i], flags, bytes)))
			}
			s.conn.Write(content)
			s.conn.Write([]byte("\r\n"))
		}
	}
	s.conn.Write([]byte("END\r\n"))
	return nil
}

///////////////////////////// STORAGE COMMANDS /////////////////////////////

func (sc *StorageCommand) parse(line []string) os.Error {
	var flags, exptime, bytes, casuniq uint64
	var err os.Error
	if len(line) < 5 {
		return os.NewError("Bad storage command: missing parameters")
	} else if flags, err = strconv.Atoui64(line[2]); err != nil {
		return os.NewError("Bad storage command: bad flags")
	} else if exptime, err = strconv.Atoui64(line[3]); err != nil {
		return os.NewError("Bad storage command: bad expiration time")
	} else if bytes, err = strconv.Atoui64(line[4]); err != nil {
		return os.NewError("Bad storage command: bad expiration time")
	} else if line[0] == "cas" {
		if casuniq, err = strconv.Atoui64(line[5]); err != nil {
			return os.NewError("Bad storage command: bad cas value")
		}
	}
	sc.command = line[0]
	sc.key = line[1]
	sc.flags = uint32(flags)
	if exptime == 0 || exptime > secondsInMonth {
		sc.exptime = uint32(exptime)
	} else {
		sc.exptime = uint32(time.Seconds()) + uint32(exptime)
	}
	sc.bytes = uint32(bytes)
	sc.cas_unique = casuniq
	if line[len(line)-1] == "noreply" {
		sc.noreply = true
	}
	return nil
}

func (sc *StorageCommand) readData(rd *bufio.Reader) os.Error {
	if sc.bytes <= 0 {
		return os.NewError("Bad storage operation: trying to read 0 bytes")
	} else {
		sc.data = make([]byte, sc.bytes+2) // \r\n is always present at the end
	}
	// read all the data
	for offset := 0; offset < int(sc.bytes); {
		if nread, err := rd.Read(sc.data[offset:]); err != nil {
			return err
		} else {
			offset += nread
		}
	}
	if string(sc.data[len(sc.data)-2:]) != "\r\n" {
		return os.NewError("Bad storage operation: bad data chunk")
	}
	sc.data = sc.data[:len(sc.data)-2] // strip \n\r
	return nil
}

func (sc *StorageCommand) Exec(s *Session) os.Error {
	logger.Printf("Storage: key: %s, flags: %d, exptime: %d, "+
		"bytes: %d, cas: %d, noreply: %t, content: %s\n",
		sc.key, sc.flags, sc.exptime, sc.bytes,
		sc.cas_unique, sc.noreply, string(sc.data))

	switch sc.command {

	case "set":
		if err := mainStorage.Set(sc.key, sc.flags, sc.exptime, sc.bytes, sc.data); err != nil {
			// This is an internal error. Connection should be closed by the server.
			s.conn.Close()
		} else if !sc.noreply {
			s.conn.Write([]byte("STORED\r\n"))
		}
		return nil
	case "add":
		if err := mainStorage.Add(sc.key, sc.flags, sc.exptime, sc.bytes, sc.data); err != nil && !sc.noreply {
			s.conn.Write([]byte("NOT_STORED\r\n"))
		} else if err == nil && !sc.noreply {
			s.conn.Write([]byte("STORED\r\n"))
		}
	case "replace":
		if err := mainStorage.Replace(sc.key, sc.flags, sc.exptime, sc.bytes, sc.data); err != nil && !sc.noreply {
			s.conn.Write([]byte("NOT_STORED\r\n"))
		} else if err == nil && !sc.noreply {
			s.conn.Write([]byte("STORED\r\n"))
		}
	case "append":
		if err := mainStorage.Append(sc.key, sc.bytes, sc.data); err != nil && !sc.noreply {
			s.conn.Write([]byte("NOT_STORED\r\n"))
		} else if err == nil && !sc.noreply {
			s.conn.Write([]byte("STORED\r\n"))
		}
	case "prepend":
		if err := mainStorage.Prepend(sc.key, sc.bytes, sc.data); err != nil && !sc.noreply {
			s.conn.Write([]byte("NOT_STORED\r\n"))
		} else if err == nil && !sc.noreply {
			s.conn.Write([]byte("STORED\r\n"))
		}
	case "cas":
		if err := mainStorage.Cas(sc.key, sc.flags, sc.exptime, sc.bytes, sc.cas_unique, sc.data); err != nil && !sc.noreply {
			//Fix this. We need special treatment for "exists" and "not found" errors.
			s.conn.Write([]byte("EXISTS\r\n"))
		} else if err == nil && !sc.noreply {
			s.conn.Write([]byte("STORED\r\n"))
		}
	}
	return nil
}
