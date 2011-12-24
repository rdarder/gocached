package main

import (
	"flag"
	"os"
	"log"
	"net"
)

var logger = log.New(os.Stdout, "gocached: ", log.Lshortfile|log.LstdFlags)
var port = flag.String("port", "11212", "memcached port")

func main() {
	mainStorage.Init(2000)
	if addr, err := net.ResolveTCPAddr("tcp", "0.0.0.0:"+*port); err != nil {
		logger.Fatalf("Unable to resolv local port %s\n", *port)
	} else if listener, err := net.ListenTCP("tcp", addr); err != nil {
		logger.Fatalln("Unable to listen on requested port")
	} else {
		logger.Printf("Starting Gocached server")
		for {
			if conn, err := listener.AcceptTCP(); err != nil {
				logger.Println("An error ocurred accepting a new connection")
			} else {
				logger.Println("New Connection")
				go clientHandler(conn)
			}
		}
	}
}

func clientHandler(conn *net.TCPConn) {
	defer conn.Close()
	session := &Session{}
	if err := session.Init(conn); err != nil {
		logger.Printf("An error %+v ocurred creating a new session", err)
	} else {
		for command, err := session.NextCommand(); err == nil; command, err = session.NextCommand() {
      logger.Printf("Command %+v", command)
			command.Exec(session)
		}
	}
}
