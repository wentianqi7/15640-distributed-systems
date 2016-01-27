package paxos

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cmu440-F15/paxosapp/rpc/paxosrpc"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"
)

type paxosNode struct {
	port        string
	idMap       map[int](peerNode) // map from paxos node id to its hostport and client
	numRetries  int
	numNodes    int
	id          int
	logicTime   int
	timeFactor  int
	nhMap       map[string](int)
	naMap       map[string](int)
	vaMap       map[string](interface{})
	valueMap    map[string](interface{})
	countReqMap map[string](chan bool)
	countResMap map[string](chan int)
	keyMutex    map[string](*sync.Mutex)
	nodeMutex   *sync.Mutex
}

type voteRequest struct {
	vote bool
	n_a  int
	v_a  interface{}
}

type voteResponse struct {
	pass bool
	n_a  int
	v_a  interface{}
}

type peerNode struct {
	client *rpc.Client
	port   string
}

// NewPaxosNode creates a new PaxosNode. This function should return only when
// all nodes have joined the ring, and should return a non-nil error if the node
// could not be started in spite of dialing the other nodes numRetries times.
//
// hostMap is a map from node IDs to their hostports, numNodes is the number
// of nodes in the ring, replace is a flag which indicates whether this node
// is a replacement for a node which failed.
func NewPaxosNode(myHostPort string, hostMap map[int]string, numNodes, srvId, numRetries int, replace bool) (PaxosNode, error) {
	fmt.Println(myHostPort)

	var timeFactor int
	for timeFactor = 10; timeFactor < numNodes; timeFactor *= 10 {
	}
	fmt.Printf("number of nodes: %d, factor: %d\n", numNodes, timeFactor)

	myPaxosNode := paxosNode{
		port:        myHostPort,
		idMap:       make(map[int](peerNode)),
		numRetries:  numRetries,
		numNodes:    numNodes,
		id:          srvId,
		logicTime:   0,
		timeFactor:  timeFactor,
		nhMap:       make(map[string](int)),
		naMap:       make(map[string](int)),
		vaMap:       make(map[string](interface{})),
		valueMap:    make(map[string](interface{})),
		countReqMap: make(map[string](chan bool)),
		countResMap: make(map[string](chan int)),
		keyMutex:    make(map[string](*sync.Mutex)),
		nodeMutex:   &sync.Mutex{},
	}

	// create listener for incoming RPCs
	listener, err := net.Listen("tcp", myHostPort)
	for err != nil {
		listener, err = net.Listen("tcp", myHostPort)
	}

	// wrap the paxos node
	err = rpc.RegisterName("PaxosNode", paxosrpc.Wrap(&myPaxosNode))
	for err != nil {
		err = rpc.RegisterName("PaxosNode", paxosrpc.Wrap(&myPaxosNode))
	}

	// set up http handler for incoming RPCs
	rpc.HandleHTTP()
	go http.Serve(listener, nil)

	for nodeId, nodePort := range hostMap {
		err = myPaxosNode.dialToPeerNode(nodeId, nodePort)
		if err != nil {
			return nil, errors.New(fmt.Sprintf("Not all paxos nodes are ready\nid: %d, port: %d", nodeId, nodePort))
		}
	}

	if replace {
		if err := myPaxosNode.replaceServer(); err != nil {
			return nil, errors.New(fmt.Sprintf("paxos node: %d fail in receiving acks", srvId))
		}

		if err := myPaxosNode.replaceCatchup(); err != nil {
			return nil, errors.New(fmt.Sprintf("paxos node: %d fail to get valueMap", srvId))
		}
	}

	return &myPaxosNode, nil
}

func (pn *paxosNode) dialToPeerNode(id int, port string) error {
	for i := 0; i < pn.numRetries; i++ {
		cli, err := rpc.DialHTTP("tcp", port)
		if err != nil {
			time.Sleep(time.Duration(1000 * time.Millisecond))
		} else {
			pn.idMap[id] = peerNode{cli, port}
			return nil
		}
	}
	return errors.New(fmt.Sprintf("Dial failed  after %d tries", pn.numRetries))
}

func (pn *paxosNode) GetNextProposalNumber(args *paxosrpc.ProposalNumberArgs, reply *paxosrpc.ProposalNumberReply) error {
	reply.N = pn.logicTime*pn.timeFactor + pn.id
	return nil
}

func (pn *paxosNode) Propose(args *paxosrpc.ProposeArgs, reply *paxosrpc.ProposeReply) error {
	fmt.Printf("[node %d enter propose]\n", pn.id)
	endTime := time.Now().Add(time.Duration(15) * time.Second)
	timeoutChan := make(chan bool)
	go pn.timing(args.Key, endTime, timeoutChan)

	pn.nodeMutex.Lock()
	if _, ok := pn.keyMutex[args.Key]; !ok {
		pn.keyMutex[args.Key] = &sync.Mutex{}
	}
	pn.nodeMutex.Unlock()

	pn.keyMutex[args.Key].Lock()
	defer pn.keyMutex[args.Key].Unlock()

	// prepare: send <prepare, myn> to all the nodes
	fmt.Printf("----------------------- ndoe %d prepare value %d n=%d\n", pn.id, args.V, args.N)
	pRequestChan := make(chan voteRequest)
	pResponseChan := make(chan voteResponse)
	go pn.voter(args.Key, pRequestChan, pResponseChan, timeoutChan)
	for _, node := range pn.idMap {
		prepareArgs := &paxosrpc.PrepareArgs{args.Key, args.N}
		var prepareReply paxosrpc.PrepareReply
		go pn.prepare(node.client, prepareArgs, &prepareReply, pRequestChan)
	}

	voteRes := <-pResponseChan
	var value interface{}
	n := args.N
	// if not recieve majority prepare-ok from the paxos nodes, terminate propose
	if !voteRes.pass {
		fmt.Println("not recieve prepare-ok from a majority")
		return nil
	} else if voteRes.n_a > args.N && voteRes.v_a != nil {
		fmt.Printf("@@@@@@@@@@@@@@@@@@ args.N=%d\n", args.N)
		fmt.Printf("value is set to %d\n", voteRes.v_a)
		value = voteRes.v_a
		n = voteRes.n_a
	} else {
		value = args.V
	}

	// accept: send <accept, myn, V> to all the nodes
	fmt.Printf("----------------------- node %d accept value %d n=%d\n", pn.id, value, n)
	aRequestChan := make(chan voteRequest)
	aResponseChan := make(chan voteResponse)
	go pn.voter(args.Key, aRequestChan, aResponseChan, timeoutChan)
	for _, node := range pn.idMap {
		acceptArgs := &paxosrpc.AcceptArgs{args.Key, n, value}
		var acceptReply paxosrpc.AcceptReply
		go pn.accept(node.client, acceptArgs, &acceptReply, aRequestChan)
	}
	voteRes = <-aResponseChan
	if !voteRes.pass {
		fmt.Println("not recieve accept-ok from a majority")
		return nil
	}

	// commit: send <commit, va> to all the nodes
	fmt.Printf("----------------------- node %d commit value %d n=%d\n", pn.id, value, n)
	cRequestChan := make(chan voteRequest)
	cResponseChan := make(chan voteResponse)
	go pn.voter(args.Key, cRequestChan, cResponseChan, timeoutChan)
	for _, node := range pn.idMap {
		commitArgs := &paxosrpc.CommitArgs{args.Key, value}
		var commitReply paxosrpc.CommitReply
		go pn.commit(node.client, commitArgs, &commitReply, cRequestChan)
	}
	<-cResponseChan
	reply.V = value
	return nil
}

func (pn *paxosNode) prepare(client *rpc.Client, args *paxosrpc.PrepareArgs, reply *paxosrpc.PrepareReply, reqChan chan voteRequest) {
	finChan := make(chan *rpc.Call, pn.numNodes)
	client.Go("PaxosNode.RecvPrepare", args, reply, finChan)
	call := <-finChan

	pReply := call.Reply.(*paxosrpc.PrepareReply)
	if pReply.Status == paxosrpc.OK {
		reqChan <- voteRequest{true, pReply.N_a, pReply.V_a}
	} else {
		reqChan <- voteRequest{false, pReply.N_a, pReply.V_a}
	}
}

func (pn *paxosNode) accept(client *rpc.Client, args *paxosrpc.AcceptArgs, reply *paxosrpc.AcceptReply, reqChan chan voteRequest) {
	finChan := make(chan *rpc.Call, pn.numNodes)
	client.Go("PaxosNode.RecvAccept", args, reply, finChan)
	call := <-finChan
	if call.Reply.(*paxosrpc.AcceptReply).Status == paxosrpc.OK {
		reqChan <- voteRequest{vote: true}
	} else {
		reqChan <- voteRequest{vote: false}
	}
}

func (pn *paxosNode) commit(client *rpc.Client, args *paxosrpc.CommitArgs, reply *paxosrpc.CommitReply, reqChan chan voteRequest) {
	finChan := make(chan *rpc.Call, pn.numNodes)
	go client.Go("PaxosNode.RecvCommit", args, reply, finChan)
	<-finChan
	reqChan <- voteRequest{vote: true}
}

func (pn *paxosNode) timing(key string, endTime time.Time, timeoutChan chan bool) {
	for {
		if time.Now().After(endTime) {
			timeoutChan <- true
			return
		}
		time.Sleep(100)
	}
}

// count the vote in a go routine
// send the result to countResChan after all the results are received
func (pn *paxosNode) voter(key string, reqChan chan voteRequest, resChan chan voteResponse, timeoutChan chan bool) {
	count := 0
	total := 0
	n_a := -1
	var v_a interface{}
	v_a = nil

	for {
		select {
		case req := <-reqChan:
			if req.vote {
				count++
				if req.n_a > n_a {
					n_a = req.n_a
					v_a = req.v_a
				}
			}
			total++
			if total == pn.numNodes {
				if count*2 > pn.numNodes {
					resChan <- voteResponse{true, n_a, v_a}
				} else {
					resChan <- voteResponse{false, n_a, nil}
				}
				return
			}
		case <-timeoutChan:
			fmt.Println("timeout!!!!!!!!!!!!!!!!!")
			resChan <- voteResponse{false, n_a, nil}
			return
		}
	}
}

func (pn *paxosNode) GetValue(args *paxosrpc.GetValueArgs, reply *paxosrpc.GetValueReply) error {
	pn.nodeMutex.Lock()
	defer pn.nodeMutex.Unlock()

	val, ok := pn.valueMap[args.Key]
	if !ok {
		reply.Status = paxosrpc.KeyNotFound
	} else {
		reply.Status = paxosrpc.KeyFound
		reply.V = val
	}
	return nil
}

func (pn *paxosNode) RecvPrepare(args *paxosrpc.PrepareArgs, reply *paxosrpc.PrepareReply) error {
	// update current logic time
	if pn.logicTime <= args.N/pn.timeFactor {
		pn.logicTime = args.N/pn.timeFactor + 1
	}

	// check if accept the prepare rpc
	if args.N < pn.nhMap[args.Key] {
		reply.Status = paxosrpc.Reject
	} else {
		pn.nhMap[args.Key] = args.N
		reply.N_a = pn.naMap[args.Key]
		reply.V_a = pn.vaMap[args.Key]
		reply.Status = paxosrpc.OK
	}
	return nil
}

func (pn *paxosNode) RecvAccept(args *paxosrpc.AcceptArgs, reply *paxosrpc.AcceptReply) error {
	// update current logic time
	if pn.logicTime <= args.N/pn.timeFactor {
		pn.logicTime = args.N/pn.timeFactor + 1
	}

	// check if accept the prepare rpc
	if args.N < pn.nhMap[args.Key] {
		reply.Status = paxosrpc.Reject
	} else {
		pn.nhMap[args.Key] = args.N
		pn.naMap[args.Key] = args.N
		pn.vaMap[args.Key] = args.V
		reply.Status = paxosrpc.OK
	}
	return nil
}

func (pn *paxosNode) RecvCommit(args *paxosrpc.CommitArgs, reply *paxosrpc.CommitReply) error {
	pn.nodeMutex.Lock()
	defer pn.nodeMutex.Unlock()

	fmt.Printf("node %d receive commit %d\n", pn.id, args.V)
	pn.valueMap[args.Key] = args.V
	return nil
}

func (pn *paxosNode) RecvReplaceServer(args *paxosrpc.ReplaceServerArgs, reply *paxosrpc.ReplaceServerReply) error {
	pn.nodeMutex.Lock()
	defer pn.nodeMutex.Unlock()
	err := pn.dialToPeerNode(args.SrvID, args.Hostport)
	if err != nil {
		return errors.New(fmt.Sprintf("[%d]: Error in acknowledging replace node!", pn.id))
	}
	return nil
}

func (pn *paxosNode) RecvReplaceCatchup(args *paxosrpc.ReplaceCatchupArgs, reply *paxosrpc.ReplaceCatchupReply) error {
	pn.nodeMutex.Lock()
	defer pn.nodeMutex.Unlock()
	var err error
	reply.Data, err = json.Marshal(pn.valueMap)
	if err != nil {
		return err
	}
	return nil
}

func (pn *paxosNode) replaceServer() error {
	// Every node in the ring should acknowledge the replacement node
	for nodeId, node := range pn.idMap {
		ackArgs := &paxosrpc.ReplaceServerArgs{pn.id, pn.port}
		ackReply := &paxosrpc.ReplaceServerReply{}
		if err := node.client.Call("PaxosNode.RecvReplaceServer", ackArgs, ackReply); err != nil {
			return errors.New(fmt.Sprintf("paxos node: %d fail to acknowledge replacement node", nodeId))
		}
	}
	return nil
}

func (pn *paxosNode) replaceCatchup() error {
	// Recover the valueMap from peerNode
	received := false
	for nodeId, node := range pn.idMap {
		if nodeId == pn.id {
			continue
		}
		catchupArgs := &paxosrpc.ReplaceCatchupArgs{}
		catchupReply := &paxosrpc.ReplaceCatchupReply{}
		if err := node.client.Call("PaxosNode.RecvReplaceCatchup", catchupArgs, catchupReply); err == nil {
			if err := json.Unmarshal(catchupReply.Data, &pn.valueMap); err == nil {
				fmt.Printf("Node %d received valueMap from node %d\n of size:%d", pn.id, nodeId, len(pn.valueMap))
				received = true
				break
			}
		}
	}

	if !received {
		return errors.New(fmt.Sprintf("paxos node: %d fail to get valueMap", pn.id))
	}

	return nil
}
