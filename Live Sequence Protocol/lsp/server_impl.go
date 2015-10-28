// Contains the implementation of a LSP server.

package lsp

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cmu440/lspnet"
	"strconv"
	"time"
)

type server struct {
	winSize          int
	cid              int
	sConn            *lspnet.UDPConn
	ticker           *time.Ticker
	epochLimit       int
	isCloseStart     bool
	idMap            map[int](clientData)
	addrMap          map[*lspnet.UDPAddr](clientData)
	addChan          chan *lspnet.UDPAddr
	dataChan         chan *Message
	readChan         chan *Message
	writeChan        chan writeData
	ackChan          chan *Message
	resendChan       chan bool
	reqReadChan      chan bool
	blockReadChan    chan bool
	closeChan        chan int
	errReadChan      chan int
	closeAllChan     chan bool
	finCloseAllChan  chan bool
	failCloseAllChan chan bool
	quitAllChan      chan bool
	mainReadBuf      []*Message
}

type clientData struct {
	connId     int
	sn         int
	addr       *lspnet.UDPAddr
	isSendData bool
	isTimeout  bool
	epochCount int
	readStart  int
	writeStart int
	readBuf    []*Message
	writeBuf   []*Message
	quitChan   chan bool
}

type writeData struct {
	connId  int
	sn      int
	payload []byte
}

// NewServer creates, initiates, and returns a new server. This function should
// NOT block. Instead, it should spawn one or more goroutines (to handle things
// like accepting incoming client connections, triggering epoch events at
// fixed intervals, synchronizing events using a for-select loop like you saw in
// project 0, etc.) and immediately return. It should return a non-nil error if
// there was an error resolving or listening on the specified port number.
func NewServer(port int, params *Params) (Server, error) {
	// init server
	myServer := server{
		winSize:          params.WindowSize,
		cid:              0,
		isCloseStart:     false,
		epochLimit:       params.EpochLimit,
		ticker:           time.NewTicker(time.Millisecond * time.Duration(params.EpochMillis)),
		idMap:            make(map[int](clientData)),
		addrMap:          make(map[*lspnet.UDPAddr](clientData)),
		addChan:          make(chan *lspnet.UDPAddr),
		dataChan:         make(chan *Message),
		readChan:         make(chan *Message),
		writeChan:        make(chan writeData),
		ackChan:          make(chan *Message),
		resendChan:       make(chan bool),
		reqReadChan:      make(chan bool),
		blockReadChan:    make(chan bool),
		closeChan:        make(chan int),
		errReadChan:      make(chan int),
		closeAllChan:     make(chan bool),
		finCloseAllChan:  make(chan bool),
		failCloseAllChan: make(chan bool),
		quitAllChan:      make(chan bool),
		mainReadBuf:      make([]*Message, 0),
	}
	// start listening
	var err error
	go func() {
		err = myServer.StartListen(strconv.Itoa(port))
	}()
	return &myServer, err
}

func (s *server) StartListen(port string) error {
	uAddr, err := lspnet.ResolveUDPAddr("udp", ":"+port)
	if err != nil {
		fmt.Println("Unable to resolve UDP addr ", port)
		return err
	}
	s.sConn, err = lspnet.ListenUDP("udp", uAddr)
	if err != nil {
		fmt.Println("Unable to listen on port ", port)
		return err
	}

	go s.handleMessages()
	go s.ticking()

	for {
		select {
		case <-s.quitAllChan:
			fmt.Println("server quit listener")
			return nil
		default:
			// read a msg from UDP and check its type
			buf := make([]byte, 2000)
			n, addr, err := s.sConn.ReadFromUDP(buf)
			if err != nil {
				return err
			}
			msg := Message{}
			json.Unmarshal(buf[0:n], &msg)
			switch msg.Type {
			case MsgConnect:
				// receive a conn msg from client
				s.addChan <- addr
			case MsgData:
				// add to data channel if hash match, drop otherwise
				if hex.EncodeToString(calMD5Hash(msg)) == hex.EncodeToString(msg.Hash) {
					s.dataChan <- &msg
				} else {
					fmt.Println("Hash does not match")
				}
			case MsgAck:
				s.ackChan <- &msg
			}
		}
	}
	return err
}

func (s *server) ticking() {
	for {
		select {
		case <-s.quitAllChan:
			fmt.Println("server quit ticker")
			s.ticker.Stop()
			return
		case <-s.ticker.C:
			s.resendChan <- true
		}
	}
}

func (s *server) handleMessages() {
	for {
		select {
		case addr := <-s.addChan:
			// receive conn request from a client
			clt, ok := s.addrMap[addr]
			if !ok {
				// register a new client if not exist in the map
				s.cid++
				// register a new client
				clt = clientData{s.cid, 0, addr, false, false, 0, 1, 1,
					make([]*Message, s.winSize+1),
					make([]*Message, 2*s.winSize+1),
					make(chan bool)}
				s.idMap[s.cid] = clt
				s.addrMap[addr] = clt
				fmt.Printf("Assign %d to %s\n", clt.connId, clt.addr.String())
			}
			msg := NewAck(clt.connId, 0)
			buf, _ := json.Marshal(msg)
			s.sConn.WriteToUDP(buf, addr)
		case <-s.resendChan:
			// 2. check if client is start to send data after conn, if not resend conn ack
			// 2. check if unacked message in write window, if yes resend
			for key := range s.idMap {
				clt, ok := s.idMap[key]
				if !ok {
					// client does not exist any more
					continue
				}
				clt.epochCount++
				if !clt.isSendData {
					// client not yet start sending data
					msg := NewAck(clt.connId, 0)
					buf, _ := json.Marshal(msg)
					s.sConn.WriteToUDP(buf, clt.addr)
				}
				// check if has unacked message in write window to each client
				for i := 0; i < s.winSize; i++ {
					msg := clt.writeBuf[i]
					if msg != nil && msg.SeqNum != -1 {
						// message not yet acked
						buf, _ := json.Marshal(msg)
						s.sConn.WriteToUDP(buf, clt.addr)
					}
				}

				// close connection if client timeout
				if clt.epochCount > s.epochLimit {
					fmt.Println("Client timeout ", clt.connId)
					clt.isTimeout = true
					if s.isCloseStart {
						// one client timeout, end sending pending data
						s.finCloseAllChan <- true
					}
				}
				s.idMap[key] = clt
				s.addrMap[clt.addr] = clt
			}
		case msg := <-s.dataChan:
			// when server read a data msg from udp, the msg is sent to data channel
			// client start sending data, mark isSendData as true
			// update the latest time receive msg from client (set epochCount to 0)
			clt, ok := s.idMap[msg.ConnID]
			if ok {
				// client starts sending data message
				clt.isSendData = true
				clt.epochCount = 0
				// send ack to client
				buf, _ := json.Marshal(NewAck(clt.connId, msg.SeqNum))
				s.sConn.WriteToUDP(buf, clt.addr)
				// position of the msg in read window
				index := msg.SeqNum - clt.readStart
				if index < 0 {
					// the message is alreay read but ack is dropped
					fmt.Println("data message already acked ", msg.SeqNum)
				} else if index < s.winSize {
					// save msg in window otherwise drop the msg
					clt.readBuf[index] = msg
					// add all in order message in read buffer to main buffer
					var i int
					for i = 0; i < s.winSize; i++ {
						if clt.readBuf[i] == nil {
							// find first data not yet received
							break
						} else {
							// inorder data - add to main buffer
							s.mainReadBuf = append(s.mainReadBuf, clt.readBuf[i])
						}
					}
					// slide window
					clt.readStart += i
					clt.readBuf = clt.readBuf[i:]
					// make sure that readBuf size is winSize+1
					for len(clt.readBuf) <= s.winSize+1 {
						clt.readBuf = append(clt.readBuf, nil)
					}
				}
				s.idMap[msg.ConnID] = clt
				s.addrMap[clt.addr] = clt
			} else {
				fmt.Println("Target client does not exist ", msg.ConnID)
			}
		case wd := <-s.writeChan:
			// write data msg to client
			clt, ok := s.idMap[wd.connId]
			if ok {
				// increment sequence number
				clt.sn++
				msg := NewData(wd.connId, clt.sn, wd.payload, nil)
				msg.Hash = calMD5Hash(*msg)
				// save data to its position in writeBuf
				index := clt.sn - clt.writeStart

				// add to window and wait for ack
				// extend buf length if needed
				if index+1 > len(clt.writeBuf) {
					clt.writeBuf = extend(clt.writeBuf, index+1)
				}
				clt.writeBuf[index] = msg
				// update map info
				s.idMap[wd.connId] = clt
				s.addrMap[clt.addr] = clt

				// send msg within window immediately
				if index < s.winSize {
					buf, _ := json.Marshal(msg)
					s.sConn.WriteToUDP(buf, clt.addr)
				}
			} else {
				fmt.Println("Target client does not exist ", wd.connId)
			}
		case msg := <-s.ackChan:
			// receive ack message from client
			// update and slide write window and send data in new window
			clt, ok := s.idMap[msg.ConnID]
			if ok {
				clt.epochCount = 0
				// seq 0 only denotes the client is still alive
				if msg.SeqNum != 0 {
					// mark the message as acked
					index := msg.SeqNum - clt.writeStart

					if index >= 0 {
						clt.writeBuf[index].SeqNum = -1
						// check if can slide window
						i := slideTo(clt.writeBuf, s.winSize)
						// slide window
						clt.writeStart += i
						clt.writeBuf = clt.writeBuf[i:]
						if len(clt.writeBuf) < s.winSize {
							clt.writeBuf = extend(clt.writeBuf, s.winSize)
						}

						// send message in the new window
						for j := 0; j < s.winSize; j++ {
							newMsg := clt.writeBuf[j]
							if newMsg != nil && newMsg.SeqNum != -1 {
								// message not yet acked
								buf, _ := json.Marshal(newMsg)
								s.sConn.WriteToUDP(buf, clt.addr)
							}
						}
					}
				}
				s.idMap[msg.ConnID] = clt
				s.addrMap[clt.addr] = clt
			} else {
				fmt.Println("Target client does not exist ", msg.ConnID)
			}
		case <-s.reqReadChan:
			// Read() send a request to read from main buffer
			if len(s.mainReadBuf) > 0 {
				msg := s.mainReadBuf[0]
				s.readChan <- msg
				s.mainReadBuf = s.mainReadBuf[1:]
			} else {
				// no data in the main buffer
				// check if any client if timeout - if yes return read err
				hasClose := false
				for key := range s.idMap {
					clt, ok := s.idMap[key]
					if ok && clt.isTimeout {
						fmt.Println("server find a closed client ", key)
						s.errReadChan <- key
						delete(s.idMap, key)
						delete(s.addrMap, clt.addr)
						hasClose = true
						break
					}
				}
				// if no timeout client, block reading
				if !hasClose {
					s.blockReadChan <- true
				}
			}
		case connId := <-s.closeChan:
			// close a specifc client connection
			clt, ok := s.idMap[connId]
			if ok {
				allAck := true
				for i := 0; i < len(clt.writeBuf); i++ {
					msg := clt.writeBuf[i]
					if msg == nil {
						break
					} else if msg.SeqNum != -1 {
						allAck = false
						break
					}
				}
				if allAck {
					fmt.Println("all writes are sent for client ", clt.connId)
					delete(s.idMap, clt.connId)
					delete(s.addrMap, clt.addr)
				}
			} else {
				fmt.Println("Client does not exist ", connId)
			}
		case <-s.closeAllChan:
			// close all client conn
			s.isCloseStart = true
			allAck := true
		LOOP:
			for connId := range s.idMap {
				clt, ok := s.idMap[connId]
				if ok {
					for i := 0; i < len(clt.writeBuf); i++ {
						msg := clt.writeBuf[i]
						if msg == nil {
							// finish sending all pending data
							delete(s.idMap, connId)
							delete(s.addrMap, clt.addr)
							// break and check next client
							break
						} else if msg.SeqNum != -1 {
							// find a unacked inorder msg, wait and block
							allAck = false
							break LOOP
						}
					}
				} else {
					// client does not exist, unregister from map
					delete(s.idMap, connId)
				}
			}
			if allAck {
				fmt.Println("finish ack all pending message to client")
				s.finCloseAllChan <- true
				return
			} else {
				s.failCloseAllChan <- true
			}
		}
	}
}

// extend the length of a slice if index out of bound
func extend(slice []*Message, length int) []*Message {
	newSlice := make([]*Message, 2*length+1)
	copy(newSlice, slice)
	return newSlice
}

// get the index which the window should slide to
func slideTo(buffer []*Message, winSize int) int {
	var i int
	for i = 0; i < winSize; i++ {
		if buffer[i] == nil || buffer[i].SeqNum != -1 {
			// not yet sent or not yet acked
			break
		}
	}
	return i
}

// calculate the md5 hash of (connId, seqNum, payload)
func calMD5Hash(msg Message) []byte {
	running_hash := md5.New()
	running_hash.Write([]byte(strconv.Itoa(msg.ConnID)))
	sum := running_hash.Sum([]byte(strconv.Itoa(msg.SeqNum)))
	sum = running_hash.Sum(msg.Payload)
	return sum
}

func (s *server) Read() (int, []byte, error) {
	// send a request to read
	s.reqReadChan <- true
	for {
		select {
		case <-s.blockReadChan:
			// no data in main buffer yet - sleep and resend request
			time.Sleep(time.Millisecond * time.Duration(10))
			s.reqReadChan <- true
		case msg := <-s.readChan:
			return msg.ConnID, msg.Payload, nil
		case connId := <-s.errReadChan:
			// 1. client conn closed
			// 2. client timeout
			// 3. server closed
			return connId, nil, errors.New("Read error")
		}
	}
}

func (s *server) Write(connID int, payload []byte) error {
	s.writeChan <- writeData{connID, 0, payload}
	return nil
}

func (s *server) CloseConn(connID int) error {
	// close a specific client conn - non blocking
	go func() {
		s.closeChan <- connID
	}()
	return nil
}

func (s *server) Close() error {
	s.closeAllChan <- true
	for {
		select {
		case <-s.finCloseAllChan:
			// all pending data are sent, return all goroutines
			close(s.quitAllChan)
			s.sConn.Close()
			return nil
		case <-s.failCloseAllChan:
			// not all pending data in writeBuf are sent
			// wait and resend the requset
			time.Sleep(time.Millisecond * time.Duration(50))
			s.closeAllChan <- true
		}
	}
	return nil
}
