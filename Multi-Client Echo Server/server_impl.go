// Implementation of a MultiEchoServer. Students should write their code in this file.

package p0

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
)

type multiEchoServer struct {
	ln        net.Listener                    // listner
	clients   map[net.Conn](chan string)      // store all the clients
	countChan chan map[net.Conn](chan string) // send count request to handler
	msgChan   chan string                     // send count result back to Count()
	ansChan   chan int                        // handle all the double messages
	addChan   chan Client                     // register new client
	rmvChan   chan net.Conn                   // remove client
	quit      chan bool                       // return goroutines before server close
}

type Client struct {
	conn net.Conn    // connection of client
	ch   chan string // buffered message channel for client
}

// New creates and returns (but does not start) a new MultiEchoServer.
func New() MultiEchoServer {
	server := multiEchoServer{
		clients:   make(map[net.Conn](chan string)),
		countChan: make(chan map[net.Conn](chan string)),
		ansChan:   make(chan int),
		msgChan:   make(chan string),
		addChan:   make(chan Client),
		rmvChan:   make(chan net.Conn),
		quit:      make(chan bool)}
	return &server
}

func (mes *multiEchoServer) Start(port int) error {
	ln, err := net.Listen("tcp", ":"+strconv.Itoa(port))
	mes.ln = ln
	if err != nil {
		fmt.Printf("fail to listen: ", err)
	}

	go acceptRoutine(mes)

	return nil
}

func (mes *multiEchoServer) Close() {
	close(mes.quit)
	mes.ln.Close()
	return
}

func (mes *multiEchoServer) Count() int {
	// send count request to handler
	go func() {
		mes.countChan <- mes.clients
	}()
	// wait for count result
	for {
		select {
		case ans := <-mes.ansChan:
			return ans
		}
	}
	return -1
}

// go routine for accepting client connections
func acceptRoutine(mes *multiEchoServer) {
	go handleMessages(mes)
	for {
		select {
		case <-mes.quit:
			return
		default:
			conn, err := mes.ln.Accept()
			if err != nil {
				continue
			}
			go handleConn(conn, mes)
		}
	}
}

// handler for channel messages, all map reads/writes are handled here
func handleMessages(mes *multiEchoServer) {
	for {
		select {
		case <-mes.quit:
			// return goroutines, close channels, and
			// close connections before server shutdown
			for conn := range mes.clients {
				if mes.clients[conn] != nil {
					close(mes.clients[conn])
					conn.Close()
				}
			}
			return
		case conn := <-mes.rmvChan:
			// close channel and delete map entry
			if mes.clients[conn] != nil {
				close(mes.clients[conn])
			}
			delete(mes.clients, conn)
		case client := <-mes.addChan:
			// register the client
			mes.clients[client.conn] = client.ch
		case clients := <-mes.countChan:
			// count the number of clients and send back to Count
			mes.ansChan <- len(clients)
		case msg := <-mes.msgChan:
			// iterate through clients
			for conn := range mes.clients {
				if mes.clients[conn] != nil {
					// drop message if the buffered channel is full
					select {
					case mes.clients[conn] <- msg:
					default:
						continue
					}
				}
			}
		}
	}
}

// handle a connection from client
func handleConn(conn net.Conn, mes *multiEchoServer) {
	defer conn.Close()
	ch := make(chan string, 75)
	// register user
	mes.addChan <- Client{conn, ch}
	// close the connection after handling finishes
	defer func() {
		mes.rmvChan <- conn
	}()
	// dealing with I/O
	go readLinesInto(conn, ch, mes)
	writeLinesFrom(conn, ch, mes)

}

// read lines from client message into message channel
func readLinesInto(conn net.Conn, ch chan<- string, mes *multiEchoServer) {
	buf := bufio.NewReader(conn)
	for {
		line, err := buf.ReadString('\n')
		// check if the connection is closed on client side
		if err != nil {
			mes.rmvChan <- conn
			return
		}
		line = formatStr(line)
		mes.msgChan <- fmt.Sprintf("%s%s\n", line, line)
	}
}

// write lines from client's channel to client
func writeLinesFrom(conn net.Conn, ch <-chan string, mes *multiEchoServer) {
	for msg := range ch {
		_, err := conn.Write([]byte(msg))
		// check if the connection is closed on client side
		if err != nil {
			mes.rmvChan <- conn
		}
	}
}

// remove trailing '\n's
func formatStr(line string) string {
	end := len(line)
	for i := len(line) - 1; i >= 0; i-- {
		if line[i] == '\n' {
			end--
		} else {
			break
		}
	}
	return line[0:end]
}
