package storageserver

import (
	//	"errors"
	"fmt"
	"sync"
	//	"strings"
	"github.com/cmu440/tribbler/libstore"
	"net"
	"os"
	"time"
	//"encoding/json"
	"github.com/cmu440/tribbler/rpc/storagerpc"
	"net/http"
	"net/rpc"
	"strconv"
)

type dataUnit struct {
	dMutex sync.Mutex /// use to lock the entire dataUnit
	//// Note that for the Delete function, should lock the whole data structure instead of this data unit.

	Set  map[string](*leaseCallBack)
	data interface{}
}

type leaseCallBack struct {
	registerTime int64
	validation   int64
	cbAddr       string
}

/// this is for a single partition.
type storageServer struct {
	totalNode int
	isMaster  bool
	nodeID    uint32

	rMutex  sync.Mutex // protects following
	ring    []storagerpc.Node
	ringSet map[storagerpc.Node](uint32) // this is a counter for the server nodes.
	//	iLocker *sync.Mutex // to lock the initialization process. This is for server only.
	//	iCond *sync.Cond
	masterReady chan int

	sMutex  sync.Mutex             // protects following
	storage map[string](*dataUnit) // use a single hashmap to simulate the key/value disk storage

	cMutex      sync.Mutex
	clientCache map[string](*rpc.Client)
}

/// find the routing address given the server ring.
func FindPartitionNode(ring []storagerpc.Node, pKey string) storagerpc.Node {

	//	fmt.Println("Partitioning with", ring)
	// extract only the real key from the pKey
	tID := libstore.StoreHash(pKey)

	firstNode, targetNode := 0, 0

	for idx, node := range ring {
		if node.NodeID < ring[firstNode].NodeID {
			firstNode = idx
		}

		if node.NodeID >= tID && (node.NodeID < ring[targetNode].NodeID || ring[targetNode].NodeID < tID) {
			targetNode = idx
		}
	}
	//
	//	fmt.Println(ring)
	//	fmt.Println("TargetID:", tID)
	//	fmt.Println("firstNode:", ring[firstNode].NodeID)
	//	fmt.Println("targetNode:", ring[targetNode].NodeID)

	if ring[targetNode].NodeID < tID {
		return ring[firstNode]
	} else {
		return ring[targetNode]
	}
}

// NewStorageServer creates and starts a new StorageServer. masterServerHostPort
// is the master storage server's host:port address. If empty, then this server
// is the master; otherwise, this server is a slave. numNodes is the total number of
// servers in the ring. port is the port number that this server should listen on.
// nodeID is a random, unsigned 32-bit ID identifying this server.
//
// This function should return only once all storage servers have joined the ring,
// and should return a non-nil error if the storage server could not be started.
func NewStorageServer(masterServerHostPort string, numNodes, port int, nodeID uint32) (StorageServer, error) {

	myHostPort, merr := os.Hostname()
	if merr != nil {
		fmt.Println("Slave server cannot get the hostname.!")
		return nil, merr
	}
	myHostPort = myHostPort + ":" + strconv.Itoa(port)

	storage := make(map[string](*dataUnit))
	clientCache := make(map[string](*rpc.Client))
	ring := make([]storagerpc.Node, 0, numNodes)
	ringSet := make(map[storagerpc.Node](uint32))
	masterReady := make(chan int)
	server := storageServer{
		isMaster:    len(masterServerHostPort) == 0,
		totalNode:   numNodes,
		ring:        ring,
		ringSet:     ringSet,
		storage:     storage,
		clientCache: clientCache,
		nodeID:      nodeID,
		masterReady: masterReady}

	//	server.iCond = sync.NewCond(&server.rMutex)

	// Create the server socket that will listen for incoming RPCs.
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	for err != nil {
		listener, err = net.Listen("tcp", fmt.Sprintf(":%d", port))
	}

	// Wrap the storage server before registering it for RPC.
	err = rpc.RegisterName("StorageServer", storagerpc.Wrap(&server))
	for err != nil {
		err = rpc.RegisterName("StorageServer", storagerpc.Wrap(&server))
	}

	// Setup the HTTP handler that will serve incoming RPCs and
	// serve requests in a background goroutine.
	rpc.HandleHTTP()
	go http.Serve(listener, nil)

	// before register for RPC, first register this node to the master node
	args := &storagerpc.RegisterArgs{storagerpc.Node{HostPort: myHostPort, NodeID: server.nodeID}}
	var reply storagerpc.RegisterReply
	if !server.isMaster {
		// a slave node
		var cli *rpc.Client
		var cerr error
		for true {
			cli, cerr = rpc.DialHTTP("tcp", masterServerHostPort)
			if cerr != nil {
				fmt.Println("@@Fatal Error: Slave server registration dial failed!!... master not established yet. ", nodeID, cerr)
			} else {
				break
			}
			time.Sleep(time.Duration(1000 * time.Millisecond))
		}

		for true {
			if err := cli.Call("StorageServer.RegisterServer", args, &reply); err != nil {
				fmt.Println("Slave registration call return error not empty.", err)
			}

			if reply.Status != storagerpc.OK {
				fmt.Println("Retry registration after one second.", server.nodeID)
				time.Sleep(time.Second)
			} else {
				server.ring = reply.Servers
				fmt.Println("Slave node successfully registered! -->", nodeID, len(server.ring))
				break
			}
		}
	} else {
		//		fmt.Println("##### Creating a master with nodeID:", nodeID)
		// a master node
		go server.RegisterServer(args, &reply)
	}

	/// if this node is the server, then block here until all the slaves are entered
	/// we need to use the conditional variable here.
	if server.isMaster {
		<-server.masterReady
		//		server.rMutex.Lock()
		//		if len(server.ring) != server.totalNode {
		//			server.iCond.Wait()
		//		}
		//		//		fmt.Println("Master node fully established!.");
		//		server.rMutex.Unlock()
	}

	return &server, nil
}

/// revoke all callbacks for the given key.
/// WARNING: Must make sure that the global sMutex is locked before calling this function!
func (ss *storageServer) RevokeCallBacksForKey(key string, data *dataUnit) error {
	//	fmt.Println("Revoking callbacks for the given key", key)

	// before we modify the value, we should call the callback to all the clients that are binded to this key
	for name, _ := range data.Set {
		//// TODO
		//// we can remove the expired connection from the map.
		//// this might help improve the performance.
		lcb := data.Set[name]
		doneChan := make(chan int)

		if lcb.registerTime+lcb.validation > time.Now().Unix() {
			// haven't expired.
			//			fmt.Println("Calling feedback:", len(data.Set), name)
			//			fmt.Println("Haven't expired:",lcb.registerTime + lcb.validation, time.Now().Unix());
			cbargs := &storagerpc.RevokeLeaseArgs{Key: key}
			var reply storagerpc.RevokeLeaseReply

			ss.cMutex.Lock()
			cb, ok := ss.clientCache[lcb.cbAddr]
			if !ok {
				var e error
				cb, e = rpc.DialHTTP("tcp", lcb.cbAddr)
				if e != nil {
					fmt.Println("FatalError: RevokeCallBacks cannot dial the callback address: ", lcb.cbAddr, e)
					ss.cMutex.Unlock()
					continue
				}
				ss.clientCache[lcb.cbAddr] = cb
			}
			ss.cMutex.Unlock()
			// run the RPC in a go routine to allow for timeouts
			go func() {
				if err := cb.Call("LeaseCallbacks.RevokeLease", cbargs, &reply); err != nil {
					//					fmt.Println("@@@@StorageServer: called back for libstore returned error!!", name, err, reply.Status);
					doneChan <- -1
				} else {
					doneChan <- 1
				}
			}()
			dur := time.Duration((lcb.registerTime + lcb.validation) - time.Now().Unix())
			//			fmt.Println("timeout length (s):",dur);
			select {
			case <-doneChan:
				//					fmt.Println("@@@@Feedback returned OK:", len(data.Set), name)
				break
			case <-time.After(dur * time.Second):
				fmt.Println("@@@@CallBack timed out.")
				break
			}

			// make the callback expire
			// lcb.registerTime = 0
		}
	}
	// or simply delete all the callbacks
	data.Set = make(map[string](*leaseCallBack))
	return nil
}

// RegisterServer adds a storage server to the ring. It replies with
// status NotReady if not all nodes in the ring have joined. Once
// all nodes have joined, it should reply with status OK and a list
// of all connected nodes in the ring.
func (ss *storageServer) RegisterServer(args *storagerpc.RegisterArgs, reply *storagerpc.RegisterReply) error {

	ss.rMutex.Lock()
	defer ss.rMutex.Unlock()

	// check if this server has already joined
	_, ok := ss.ringSet[args.ServerInfo]
	if !ok {
		fmt.Println("MasterSide: Register a new Slave Storage Server.", args.ServerInfo)
		ss.ring = append(ss.ring, args.ServerInfo)
		ss.ringSet[args.ServerInfo] = 1
	} else {
		//		fmt.Println("MasterSide: Ignore one slave storage server.", args.ServerInfo);
	}

	if len(ss.ring) == ss.totalNode {
		// if the connection is finished
		reply.Servers = ss.ring
		reply.Status = storagerpc.OK
		//ss.iCond.Signal()
		if !ok {
			ss.masterReady <- 1
		}
		fmt.Println("****Register Function: all node registered.")
	} else {
		reply.Status = storagerpc.NotReady
		//		fmt.Println("Registration not ready:", len(ss.ring), ss.totalNode)
	}

	return nil
}

// GetServers retrieves a list of all connected nodes in the ring. It
// replies with status NotReady if not all nodes in the ring have joined.
/// TODO this funcion might not need the mutex at all.
func (ss *storageServer) GetServers(args *storagerpc.GetServersArgs, reply *storagerpc.GetServersReply) error {

	//	fmt.Println("GetServer Called")

	ss.rMutex.Lock()
	defer ss.rMutex.Unlock()

	if !ss.isMaster {
		fmt.Println("!!!!!!!!! The get server is called in the slave node!!!")
	}

	if len(ss.ring) == ss.totalNode {
		// if the connection is finished
		reply.Servers = ss.ring
		reply.Status = storagerpc.OK

		//fmt.Println(reply.Servers);
	} else {
		reply.Status = storagerpc.NotReady
	}

	return nil
}

// Get retrieves the specified key from the data store and replies with
// the key's value and a lease if one was requested. If the key does not
// fall within the storage server's range, it should reply with status
// WrongServer. If the key is not found, it should reply with status
// KeyNotFound.
func (ss *storageServer) Get(args *storagerpc.GetArgs, reply *storagerpc.GetReply) error {

	//	fmt.Println("Get function is called.", args);
	if FindPartitionNode(ss.ring, args.Key).NodeID != ss.nodeID {
		reply.Status = storagerpc.WrongServer
		fmt.Println("GetFunction: Wrong Server")
		return nil
	}

	// first get the value from the storage map
	ss.sMutex.Lock()
	data, ok := ss.storage[args.Key]
	ss.sMutex.Unlock()

	if !ok {
		// if the key is not found
		reply.Status = storagerpc.KeyNotFound
		//		fmt.Println("@@@@GetFunction: key not found!")
		return nil //errors.New("Key not found for Get Function.");
	} else {
		data.dMutex.Lock()
		defer data.dMutex.Unlock()

		if args.WantLease {
			//			fmt.Println("GetFunction: Granting Lease.",args.HostPort,args.Key);
			// want a lease
			// grant a lease
			reply.Status = storagerpc.OK
			reply.Value = data.data.(string)
			reply.Lease.Granted = true
			reply.Lease.ValidSeconds = storagerpc.LeaseSeconds

			if data.Set == nil {
				lMap := make(map[string](*leaseCallBack))
				data.Set = lMap
			}

			// add this one to the lease list
			var val *leaseCallBack
			val, ok = data.Set[args.HostPort]
			if ok {
				// only update the lease time, don't need to create the hanlder
				val.registerTime = time.Now().Unix()
			} else {
				// need to create a new leaseCallBack struct
				//				cli, err := rpc.DialHTTP("tcp", args.HostPort)
				//				if err != nil {
				//					fmt.Println("GetFunction add callback lease failed.")
				//					return err
				//				}

				val = &leaseCallBack{
					//					cli:          cli,
					cbAddr:       args.HostPort,
					registerTime: time.Now().Unix(),
					validation:   (storagerpc.LeaseGuardSeconds + storagerpc.LeaseSeconds)}

				data.Set[args.HostPort] = val
			}
		} else {
			reply.Status = storagerpc.OK
			reply.Value = data.data.(string)
			reply.Lease.Granted = false
		}
	}
	//	fmt.Println("Get function returns.", reply.Value);
	return nil
}

// Delete remove the specified key from the data store.
// If the key does not fall within the storage server's range,
// it should reply with status WrongServer.
// If the key is not found, it should reply with status KeyNotFound.
func (ss *storageServer) Delete(args *storagerpc.DeleteArgs, reply *storagerpc.DeleteReply) error {

	//	fmt.Println("Delete function Called",args)
	if FindPartitionNode(ss.ring, args.Key).NodeID != ss.nodeID {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	// lock the whole storage first
	ss.sMutex.Lock()
	defer ss.sMutex.Unlock()

	// retrieve the data unit
	data, ok := ss.storage[args.Key]
	if ok {
		reply.Status = storagerpc.OK
		data.dMutex.Lock()
		// WARNING: here we might meet the situation of wild pointer.
		// that is the value is already deleted from the map, but the
		ss.RevokeCallBacksForKey(args.Key, data)
		data.dMutex.Unlock()

		// aplly changes
		delete(ss.storage, args.Key)

		return nil
	} else {
		reply.Status = storagerpc.KeyNotFound
		return nil
	}

	return nil
}

func (ss *storageServer) GetList(args *storagerpc.GetArgs, reply *storagerpc.GetListReply) error {

	//	fmt.Println("GetList Function Called!.", args);
	if FindPartitionNode(ss.ring, args.Key).NodeID != ss.nodeID {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	reply.Lease.Granted = false

	// first get the value from the storage map
	ss.sMutex.Lock()
	data, ok := ss.storage[args.Key]
	ss.sMutex.Unlock()

	if !ok {
		// if the key is not found
		reply.Status = storagerpc.KeyNotFound
		return nil //errors.New("Key not found for GetList Function.")
	} else {
		data.dMutex.Lock()
		defer data.dMutex.Unlock()

		if args.WantLease {
			// want a lease
			// grant a lease
			reply.Lease = storagerpc.Lease{Granted: true, ValidSeconds: storagerpc.LeaseSeconds}
			reply.Status = storagerpc.OK
			reply.Value = data.data.([]string)

			if data.Set == nil {
				lMap := make(map[string](*leaseCallBack))
				data.Set = lMap
			}

			// add this one to the lease list
			var val *leaseCallBack
			val, ok = data.Set[args.HostPort]
			if ok {
				// only update the lease time, don't need to create the hanlder
				val.registerTime = time.Now().Unix()
			} else {
				// need to create a new leaseCallBack struct
				//				cli, err := rpc.DialHTTP("tcp", args.HostPort)
				//				if err != nil {
				//					fmt.Println("GetFunction add callback lease failed.")
				//					return err
				//				}

				val = &leaseCallBack{
					//					cli:          cli,
					cbAddr:       args.HostPort,
					registerTime: time.Now().Unix(),
					validation:   storagerpc.LeaseGuardSeconds + storagerpc.LeaseSeconds}

				data.Set[args.HostPort] = val
			}
		} else {
			reply.Status = storagerpc.OK
			reply.Value = data.data.([]string)
		}
	}
	//	fmt.Println("Get function returns.", reply.Value);
	return nil
}

/// TODO: add the lease function
func (ss *storageServer) Put(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {

	//	fmt.Println("Put function is called.", args);
	if FindPartitionNode(ss.ring, args.Key).NodeID != ss.nodeID {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	ss.sMutex.Lock()
	data, ok := ss.storage[args.Key]
	if ok {
		ss.sMutex.Unlock()
		//		fmt.Println("Put function called: Update a value")
		reply.Status = storagerpc.OK

		// need to deal with the revoke
		data.dMutex.Lock()
		ss.RevokeCallBacksForKey(args.Key, data)

		// apply changes
		// note that the data might be already deleted, so not in the storage map..
		data.data = args.Value
		data.dMutex.Unlock()
	} else {
		//		fmt.Println("Put function called: Insert a value")
		reply.Status = storagerpc.OK
		data = &dataUnit{data: args.Value, Set: nil}
		ss.storage[args.Key] = data

		ss.sMutex.Unlock()
	}
	return nil
}

/// return whether the element is inside of the list or not.
/// if not, return -1
/// otherwise return the index of the element.
func (ss *storageServer) ListContains(list []string, ele string) int {
	for i := 0; i < len(list); i++ {
		if list[i] == ele {
			return i
		}
	}
	return -1
}

// AppendToList retrieves the specified key from the data store and appends
// the specified value to its list. If the key does not fall within the
// receiving server's range, it should reply with status WrongServer. If
// the specified value is already contained in the list, it should reply
// with status ItemExists.
func (ss *storageServer) AppendToList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	//	fmt.Println("AppendToList function called:", args);
	if FindPartitionNode(ss.ring, args.Key).NodeID != ss.nodeID {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	ss.sMutex.Lock()

	var list []string
	data, ok := ss.storage[args.Key]
	if !ok {
		//		fmt.Println("First time insert something to the list.. initialize it");
		list = make([]string, 0, 1)
		list = append(list, args.Value)

		ss.storage[args.Key] = &dataUnit{data: list, Set: nil}
		ss.sMutex.Unlock()
		reply.Status = storagerpc.OK
	} else {
		ss.sMutex.Unlock()
		data.dMutex.Lock()

		list = data.data.([]string)
		if ss.ListContains(list, args.Value) != -1 {
			reply.Status = storagerpc.ItemExists
		} else {
			// call the revoke call backs!
			ss.RevokeCallBacksForKey(args.Key, data)
			reply.Status = storagerpc.OK
			// apply changes
			list = append(list, args.Value)
			data.data = list
		}
		data.dMutex.Unlock()
	}

	return nil
}

// RemoveFromList retrieves the specified key from the data store and removes
// the specified value from its list. If the key does not fall within the
// receiving server's range, it should reply with status WrongServer. If
// the specified value is not already contained in the list, it should reply
// with status ItemNotFound.
func (ss *storageServer) RemoveFromList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	//	fmt.Println("RemoveFromList function called:", args);
	if FindPartitionNode(ss.ring, args.Key).NodeID != ss.nodeID {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	ss.sMutex.Lock()
	data, ok := ss.storage[args.Key]
	ss.sMutex.Unlock()
	if !ok {
		reply.Status = storagerpc.KeyNotFound
	} else {
		data.dMutex.Lock()

		list := data.data.([]string)
		index := ss.ListContains(list, args.Value)
		if index == -1 {
			reply.Status = storagerpc.ItemNotFound
		} else {
			reply.Status = storagerpc.OK
			ss.RevokeCallBacksForKey(args.Key, data)
			list = append(list[:index], list[index+1:]...)
			data.data = list
		}

		data.dMutex.Unlock()
	}

	return nil
}
