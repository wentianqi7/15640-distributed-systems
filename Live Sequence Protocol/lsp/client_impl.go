// Contains the implementation of a LSP client.

package lsp

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cmu440/lspnet"
	"time"
)

type client struct {
	winSize       int             // window size
	cid           int             // connId
	sn            int             // sequence number
	sAddr         *lspnet.UDPAddr // server UDP address
	conn          *lspnet.UDPConn // UDP connection
	ticker        *time.Ticker    // epoch event ticker
	epochLimit    int             // number of tick intervals before timeout
	connSucChan   chan Message    // if resend conn get ack, add to this chan
	dataChan      chan *Message
	ackChan       chan *Message
	readChan      chan *Message // send data to Read()
	writeChan     chan clientWriteData
	reqidChan     chan bool // request to get connId
	getidChan     chan int  // response of connId
	setidChan     chan int  // set connId and notify conn success
	isSendData    bool      // true if start receiving data from server
	isConn        bool      // true if conn is established
	isTimeout     bool      // true if client timeout
	epochCount    int       // # tick intervals not receiving msg from server
	readStart     int       // start seqNum of read buffer
	writeStart    int       // start seqNum of write buffer
	readBuf       []*Message
	writeBuf      []*Message
	mainReadBuf   []*Message // all inorder data ready to read
	resendChan    chan bool  // check if resend is need every tick
	reqReadChan   chan bool
	blockReadChan chan bool
	quitChan      chan bool
	reqCloseChan  chan bool
	finCloseChan  chan error
	failCloseChan chan bool
	errReadChan   chan bool // notify Read() and return err
}

type clientWriteData struct {
	mt      MsgType // message type (conn, data, ack)
	sn      int     // sequence number
	payload []byte  //data to write
}

// NewClient creates, initiates, and returns a new client. This function
// should return after a connection with the server has been established
// (i.e., the client has received an Ack message from the server in response
// to its connection request), and should return a non-nil error if a
// connection could not be made (i.e., if after K epochs, the client still
// hasn't received an Ack message from the server in response to its K
// connection requests).
//
// hostport is a colon-separated string identifying the server's host address
// and port number (i.e., "localhost:9999").
func NewClient(hostport string, params *Params) (Client, error) {
	// init client
	myClient := client{
		winSize:       params.WindowSize,
		sn:            0,
		ticker:        time.NewTicker(time.Millisecond * time.Duration(params.EpochMillis)),
		epochLimit:    params.EpochLimit,
		connSucChan:   make(chan Message),
		dataChan:      make(chan *Message),
		ackChan:       make(chan *Message),
		readChan:      make(chan *Message),
		writeChan:     make(chan clientWriteData),
		reqidChan:     make(chan bool),
		getidChan:     make(chan int),
		setidChan:     make(chan int),
		isSendData:    false,
		isConn:        false,
		isTimeout:     false,
		epochCount:    0,
		readStart:     1,
		writeStart:    1,
		readBuf:       make([]*Message, params.WindowSize+1),
		writeBuf:      make([]*Message, 2*params.WindowSize+1),
		mainReadBuf:   make([]*Message, 0),
		resendChan:    make(chan bool),
		reqReadChan:   make(chan bool),
		blockReadChan: make(chan bool),
		quitChan:      make(chan bool),
		reqCloseChan:  make(chan bool),
		finCloseChan:  make(chan error),
		failCloseChan: make(chan bool),
		errReadChan:   make(chan bool),
	}
	// start dial to server
	err := myClient.StartDial(hostport)
	return &myClient, err
}

func (c *client) StartDial(hostport string) error {
	var err error
	c.sAddr, err = lspnet.ResolveUDPAddr("udp", hostport)
	if err != nil {
		fmt.Println("Unable to resolve server UDP addr ", hostport)
		return err
	}
	go c.handleMessages()
	go c.ticking()

	c.conn, err = lspnet.DialUDP("udp", nil, c.sAddr)

	if err != nil {
		fmt.Println("cannot dial to port ", hostport)
		return err
	}

	fmt.Println("dial to ", hostport)

	// send connect message
	c.writeChan <- clientWriteData{MsgConnect, 0, nil}

	// waiting for connect ack
	buf := make([]byte, 2000)
	msg := Message{}
	n, err := c.conn.Read(buf)
	if err != nil {
		fmt.Println("unable read from connection")
		msg = <-c.connSucChan
	} else {
		json.Unmarshal(buf[0:n], &msg)
	}
	switch msg.Type {
	case MsgAck:
		c.setidChan <- msg.ConnID
	default:
		return errors.New("unable to connect")
	}

	// start listen on data/ack
	go c.StartListen()

	return nil
}

func (c *client) StartListen() error {
	for {
		select {
		case <-c.quitChan:
			fmt.Println("client quit listener")
			return nil
		default:
			buf := make([]byte, 2000)
			n, err := c.conn.Read(buf)
			if err != nil {
				// client cannot read message from the connection
				return err
			}
			msg := Message{}
			json.Unmarshal(buf[0:n], &msg)
			switch msg.Type {
			case MsgData:
				// drop the data message if hash does not match, otherwise send to data channel
				if hex.EncodeToString(calMD5Hash(msg)) == hex.EncodeToString(msg.Hash) {
					c.dataChan <- &msg
				} else {
					fmt.Println("Hash does not match")
				}
			case MsgAck:
				// receive ack from server
				c.ackChan <- &msg
			}
		}
	}
	return nil
}

func (c *client) ticking() {
	for {
		select {
		case <-c.quitChan:
			fmt.Println("client quit ticker")
			c.ticker.Stop()
			return
		case <-c.ticker.C:
			c.resendChan <- true
		}
	}
}

func (c *client) handleMessages() {
	for {
		select {
		case <-c.resendChan:
			// 1. check if received conn ack from server, if not resend conn msg
			// 2. check if server is start to send data, if not send hearbeat ack
			// 2. check if unacked message in write window, if yes resend
			c.epochCount++
			if !c.isConn {
				// server not yet send connection ack, resend connection message
				wbuf, _ := json.Marshal(NewConnect())
				c.conn.Write(wbuf)
				// block and read ack from server
				rbuf := make([]byte, 2000)
				n, err := c.conn.Read(rbuf)
				msg := Message{}
				if err == nil {
					json.Unmarshal(rbuf[0:n], &msg)
					if msg.Type == MsgAck {
						c.connSucChan <- msg
					}
				}
			} else {
				if !c.isSendData {
					// server not yet start sending data
					msg := NewAck(c.cid, 0)
					buf, _ := json.Marshal(msg)
					c.conn.Write(buf)
				}
				// check if client has unacked message in window
				for i := 0; i < c.winSize; i++ {
					msg := c.writeBuf[i]
					if msg != nil && msg.SeqNum != -1 {
						// message not yet acked, resend
						buf, _ := json.Marshal(msg)
						c.conn.Write(buf)
					}
				}
			}
			// close connection if client timeout
			if c.epochCount > c.epochLimit {
				// close the client
				c.isTimeout = true
				close(c.quitChan)
				c.conn.Close()
			}
		case msg := <-c.dataChan:
			// when client read a data msg from udp, the msg is sent to data channel
			// server start sending data, mark isSendData as true
			// update the latest time receive msg from server (set epochCount to 0)
			c.isSendData = true
			c.epochCount = 0
			// write ack to server
			buf, _ := json.Marshal(NewAck(c.cid, msg.SeqNum))
			c.conn.Write(buf)
			index := msg.SeqNum - c.readStart
			if index < 0 {
				// the message is alreay read but ack is dropped
				fmt.Println("data message already acked ", msg.SeqNum)
			} else if index < c.winSize {
				// save msg in window otherwise drop the msg
				c.readBuf[index] = msg
				// add all in order message in read buffer to main buffer
				var i int
				for i = 0; i < c.winSize; i++ {
					if c.readBuf[i] == nil {
						// the first messgae not yet received
						break
					} else {
						// inorder msg - append to main buffer
						c.mainReadBuf = append(c.mainReadBuf, c.readBuf[i])
					}
				}
				// slide window
				c.readStart += i
				c.readBuf = c.readBuf[i:]
				// make sure that readBuf size is winSize+1
				for len(c.readBuf) < c.winSize+1 {
					c.readBuf = append(c.readBuf, nil)
				}
			}
		case msg := <-c.ackChan:
			// receive ack message from server
			// update and slide write window and send data in new window
			c.epochCount = 0
			c.isSendData = true
			// seq 0 only denotes ack to conn msg
			if msg.SeqNum != 0 {
				// mark the message as acked
				index := msg.SeqNum - c.writeStart
				if index >= 0 {
					c.writeBuf[index].SeqNum = -1
					// check if can slide window
					i := slideTo(c.writeBuf, c.winSize)
					// slide window
					c.writeStart += i
					c.writeBuf = c.writeBuf[i:]
					// make sure write buffer length greater than winSize
					if len(c.writeBuf) < c.winSize {
						c.writeBuf = extend(c.writeBuf, c.winSize)
					}
					// send message in the new window
					for j := 0; j < c.winSize; j++ {
						newMsg := c.writeBuf[j]
						if newMsg != nil && newMsg.SeqNum != -1 {
							// message not yet acked
							buf, _ := json.Marshal(newMsg)
							c.conn.Write(buf)
						}
					}
				}
			}
		case wd := <-c.writeChan:
			// msg to write
			switch wd.mt {
			case MsgConnect:
				buf, _ := json.Marshal(NewConnect())
				c.conn.Write(buf)
			case MsgData:
				// seqNum is incremented only when writing data msg
				c.sn++
				msg := NewData(c.cid, c.sn, wd.payload, nil)
				msg.Hash = calMD5Hash(*msg)
				// save data to its position in writeBuf
				index := c.sn - c.writeStart
				// add to window and wait for ack
				// extend buf length if needed
				if index+1 > len(c.writeBuf) {
					c.writeBuf = extend(c.writeBuf, index+1)
				}
				c.writeBuf[index] = msg
				// send msg within window immediately
				if index < c.winSize {
					buf, _ := json.Marshal(msg)
					c.conn.Write(buf)
				}
			case MsgAck:
				// ack use seqNum of original data msg
				buf, _ := json.Marshal(NewAck(c.cid, wd.sn))
				c.conn.Write(buf)
			}
		case <-c.reqReadChan:
			// Read() send a request to read from main buffer
			// if client is timeout, read error
			if c.isTimeout {
				c.errReadChan <- true
				return
			}
			if len(c.mainReadBuf) > 0 {
				// have data in main buffer, send to read channel
				c.readChan <- c.mainReadBuf[0]
				c.mainReadBuf = c.mainReadBuf[1:]
			} else {
				// blocking in Read()
				c.blockReadChan <- true
			}
		case id := <-c.setidChan:
			// conn succesfful
			// set the connId of client
			c.cid = id
			c.isConn = true
		case <-c.reqidChan:
			// request get connId
			c.getidChan <- c.cid
		case <-c.reqCloseChan:
			// request to close the client
			allAck := true
			// send pending data in writeBuf before close
			for i := 0; i < len(c.writeBuf); i++ {
				msg := c.writeBuf[i]
				if msg == nil {
					// all inorder pending data are sent
					break
				} else if msg.SeqNum != -1 {
					// unacked data found
					allAck = false
					break
				}
			}
			if allAck {
				fmt.Println("all writes are sent for server")
				c.finCloseChan <- nil
			} else {
				// block and check if all data are sent later
				c.failCloseChan <- true
			}
		}
	}
}

func (c *client) ConnID() int {
	go func() {
		c.reqidChan <- true
	}()

	for {
		select {
		case <-c.quitChan:
			return 0
		case id := <-c.getidChan:
			return id
		}
	}
	return 0
}

func (c *client) Read() ([]byte, error) {
	// send a request to read
	c.reqReadChan <- true
	for {
		select {
		case <-c.blockReadChan:
			// nothing in the main buffer - blocking
			time.Sleep(time.Millisecond * time.Duration(10))
			c.reqReadChan <- true
		case msg := <-c.readChan:
			// read success, return the data
			return msg.Payload, nil
		case <-c.errReadChan:
			// read error
			// 1. client close called
			// 2. client timeout
			return nil, errors.New("read error")
		}
	}
}

func (c *client) Write(payload []byte) error {
	c.writeChan <- clientWriteData{MsgData, 0, payload}
	return nil
}

func (c *client) Close() error {
	// send close request
	c.reqCloseChan <- true
	for {
		select {
		case <-c.failCloseChan:
			// not all pending data in writeBuf are sent
			// wait and resend the requset
			time.Sleep(time.Millisecond * time.Duration(10))
			c.reqCloseChan <- true
		case err := <-c.finCloseChan:
			// all pending data are sent, return all goroutines
			close(c.quitChan)
			c.conn.Close()
			fmt.Println("client connection closed ", c.cid)
			return err
		}
	}
}
