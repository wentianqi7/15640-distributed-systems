package libstore

import (
	"errors"
	"fmt"
	"github.com/cmu440/tribbler/rpc/storagerpc"
	"net/rpc"
	//	"strings"
	//	"net"
	//	"net/http"
	"github.com/cmu440/tribbler/rpc/librpc"
	"sync"
	"time"
)

// the data unit in the cache
type dataUnit struct {
	//	requestQ []int64
	expireTime int64
	data       interface{}
}

type storageNode struct {
	NodeID uint32
	conn   *rpc.Client
}

type libstore struct {
	storages []storageNode
	callback string
	mode     LeaseMode

	/// simply lock the whole cache.
	dMutex    sync.Mutex             /// the mutex used for the following cache.
	dataCache map[string](*dataUnit) /// the data(reply) may not be cached while the query is always cache.!

	qMutex     sync.Mutex /// used for the query cache
	queryCache map[string]([]int64)
}

func (ls *libstore) isExpired(expireTime int64) bool {
	return time.Now().UnixNano() > expireTime
}

func (ls *libstore) GarbageCollection() {
	// every time out seconds, run garbage collection to release the cache
	for {
		time.Sleep(storagerpc.LeaseSeconds * time.Second)
		fmt.Println("**LIBSTORE: Running Garbage Collection.**")

		ls.dMutex.Lock()
		for key, data := range ls.dataCache {
			if ls.isExpired(data.expireTime) {
				delete(ls.dataCache, key)
			}
		}
		ls.dMutex.Unlock()

		ls.qMutex.Lock()
		for qk, qd := range ls.queryCache {
			if qd[len(qd)-1]+storagerpc.QueryCacheSeconds*int64(time.Second) < time.Now().UnixNano() {
				delete(ls.queryCache, qk)
			}
		}
		ls.qMutex.Unlock()
	}
}

// initialize the server connection for all the storage servers
func WrapNode(nodes []storagerpc.Node, ret []storageNode) error {
	//	fmt.Println("Transfering the storagenodes:", nodes)
	for index, val := range nodes {
		cli, err := rpc.DialHTTP("tcp", val.HostPort)
		if err != nil {
			fmt.Println("@@@@FatalError: the storage server cannot be connected!!")
			return err
		}
		ret[index] = storageNode{conn: cli, NodeID: val.NodeID}
	}

	return nil
}

/// update the query cache,
/// call this function only when the mode is not Never.
func (ls *libstore) updateQueryCache(key string) bool {
	ls.qMutex.Lock()
	defer ls.qMutex.Unlock()

	data, ok := ls.queryCache[key]
	if ok {
		// update the query listv
		//		fmt.Println("Cache: entering checkdate", data)
		data = append(data, time.Now().UnixNano())
		//		fmt.Println("Cache: before checkdate", data)
		offset := 0
		for i, _ := range data {
			if data[i]+storagerpc.QueryCacheSeconds*int64(time.Second) < time.Now().UnixNano() {
				offset++
			} else {
				break
			}
		}
		data = data[offset:]

		if len(data) > storagerpc.QueryCacheThresh {
			// truncate the queue
			data = data[len(data)-storagerpc.QueryCacheThresh:]
		}

		ls.queryCache[key] = data
		//		fmt.Println("Cache: after checkdate", data)
	} else {
		// cache the query
		requestQ := make([]int64, 0, 1)
		requestQ = append(requestQ, time.Now().UnixNano())
		ls.queryCache[key] = requestQ
	}

	// check if we need to request lease or not
	// if the key is not cached and L has sent QueryCacheThresh or more queries for the key
	// in the last QueryCacheSeconds, then send a request to S with the WantLease flag set
	// to true. When the reply comes back, L should insert the result into its local cache
	// before returning it to the caller.
	if len(data) >= storagerpc.QueryCacheThresh {
		return true
	} else {
		return false
	}
}

/// find the routing address given the server ring.
func (ls *libstore) FindPartitionNode(ring []storageNode, pKey string) storageNode {

	// extract only the real key from the pKey
	tID := StoreHash(pKey)

	firstNode, targetNode := 0, 0

	for idx, node := range ring {
		if node.NodeID < ring[firstNode].NodeID {
			firstNode = idx
		}

		if node.NodeID >= tID && (node.NodeID < ring[targetNode].NodeID || ring[targetNode].NodeID < tID) {
			targetNode = idx
		}
	}

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

// NewLibstore creates a new instance of a TribServer's libstore. masterServerHostPort
// is the master storage server's host:port. myHostPort is this Libstore's host:port
// (i.e. the callback address that the storage servers should use to send back
// notifications when leases are revoked).
//
// The mode argument is a debugging flag that determines how the Libstore should
// request/handle leases. If mode is Never, then the Libstore should never request
// leases from the storage server (i.e. the GetArgs.WantLease field should always
// be set to false). If mode is Always, then the Libstore should always request
// leases from the storage server (i.e. the GetArgs.WantLease field should always
// be set to true). If mode is Normal, then the Libstore should make its own
// decisions on whether or not a lease should be requested from the storage server,
// based on the requirements specified in the project PDF handout.  Note that the
// value of the mode flag may also determine whether or not the Libstore should
// register to receive RPCs from the storage servers.
//
// To register the Libstore to receive RPCs from the storage servers, the following
// line of code should suffice:
//
//     rpc.RegisterName("LeaseCallbacks", librpc.Wrap(libstore))
//
// Note that unlike in the NewTribServer and NewStorageServer functions, there is no
// need to create a brand new HTTP handler to serve the requests (the Libstore may
// simply reuse the TribServer's HTTP handler since the two run in the same process).
func NewLibstore(masterServerHostPort, myHostPort string, mode LeaseMode) (Libstore, error) {

	if myHostPort == "" && mode != Never {
		fmt.Println("@@@@FatalBughhh")
	}

	cli, err := rpc.DialHTTP("tcp", masterServerHostPort)
	if err != nil {
		return nil, err
	}

	// keep retrying for at most 5 times.
	args := &storagerpc.GetServersArgs{}
	var storages []storageNode
	for i := 0; ; i++ {
		var reply storagerpc.GetServersReply
		if cli.Call("StorageServer.GetServers", args, &reply) != nil {
			fmt.Println("@@@@FatalError: libstore RPC function call failed. -> GetServers!")
		}

		if reply.Status == storagerpc.OK {
			storages = make([]storageNode, len(reply.Servers), len(reply.Servers))
			err = WrapNode(reply.Servers, storages)
			//			fmt.Println("After the wrapping:", storages)
			if err != nil {
				return nil, err
			}
			break
		}

		fmt.Println("LibStore retrying connection...", i)
		time.Sleep(time.Second)

		if i == 6 {
			fmt.Println("@@@@FatalError: libstore has tried 5 times, but the master node is still not ready!")
			return nil, err
		}

	}

	dCache := make(map[string](*dataUnit))
	qCache := make(map[string]([]int64))

	lib := &libstore{
		storages:   storages,
		callback:   myHostPort,
		mode:       mode,
		dataCache:  dCache,
		queryCache: qCache}

	// register call back if needed
	if mode != Never {
		if err = rpc.RegisterName("LeaseCallbacks", librpc.Wrap(lib)); err != nil {
			fmt.Println("@@@@Fatal Error, cannot register the libstore's RPC", err)
			return nil, err
		}
	}

	go lib.GarbageCollection()

	fmt.Println("Libstore created:", lib.callback)
	return lib, nil
}

/// get a value for the given key.
func (ls *libstore) Get(key string) (string, error) {
	//	fmt.Println("Get Called: ", key, ls.callback)
	/// check query status
	var wantlease bool
	if ls.mode == Always {
		//		fmt.Println("GETfunction: Always ask for lease.")
		wantlease = true
	} else if ls.mode == Never {
		//		fmt.Println("GETfunction: Nerver ask for lease.")
		wantlease = false
	} else {
		wantlease = ls.updateQueryCache(key)
	}

	// check data cache
	ls.dMutex.Lock()
	defer ls.dMutex.Unlock()

	data, ok := ls.dataCache[key]
	if ok {
		if !ls.isExpired(data.expireTime) {
			// valid cache, return immediately
			//			fmt.Println("GetFunctionCalled: use cache..", key)
			return data.data.(string), nil
		}
		//		fmt.Println("GetFunctionCalled: cache expired!!", key)
	}

	//	fmt.Println("GetFunctionCalled: no cache detected, grab key", key)

	args := &storagerpc.GetArgs{Key: key, WantLease: wantlease, HostPort: ls.callback}
	var reply storagerpc.GetReply
	targetNode := ls.FindPartitionNode(ls.storages, key)
	if err := targetNode.conn.Call("StorageServer.Get", args, &reply); err != nil {
		return "Get Failed.", err
	}

	if reply.Status != storagerpc.OK {
		fmt.Println("Reply status is not OK.!")
		return "", errors.New("Not fine")
	} else {
		// cache the data before return
		// note that the lease should alwasy be granted
		if reply.Lease.Granted != args.WantLease {
			//			fmt.Println("@@@@@@@@@@#### Get Function: Nasty Error:Why is the lease not granted???? Granted?", reply.Lease.Granted);
		} else if wantlease {
			ls.dataCache[key] = &dataUnit{expireTime: time.Now().UnixNano() + int64(reply.Lease.ValidSeconds)*int64(time.Second), data: reply.Value}
		}

		return reply.Value, nil
	}
}

/// put a value to the storage server.
func (ls *libstore) Put(key, value string) error {
	//	fmt.Println("Put Called: ", key, ls.callback)

	// check data cache
	//	ls.dMutex.Lock()
	//	_, ok := ls.dataCache[key]
	//	if ok {
	//		delete(ls.dataCache, key)
	//	}
	//	ls.dMutex.Unlock()

	args := &storagerpc.PutArgs{Key: key, Value: value}
	var reply storagerpc.PutReply
	server := ls.FindPartitionNode(ls.storages, key)
	if err := server.conn.Call("StorageServer.Put", args, &reply); err != nil {
		return err
	}

	if reply.Status != storagerpc.OK {
		return errors.New("Not fine")
	} else {
		return nil
	}
}

/// delete a key from the storage..
func (ls *libstore) Delete(key string) error {
	//	fmt.Println("Delete Called: ", key, ls.callback)

	// check data cache
	//	ls.dMutex.Lock()
	//	_, ok := ls.dataCache[key]
	//	if ok {
	//		delete(ls.dataCache, key)
	//	}
	//	ls.dMutex.Unlock()

	args := &storagerpc.DeleteArgs{Key: key}
	var reply storagerpc.DeleteReply
	if err := ls.FindPartitionNode(ls.storages, key).conn.Call("StorageServer.Delete", args, &reply); err != nil {
		return err
	}

	if reply.Status != storagerpc.OK {
		return errors.New("Not fine")
	} else {
		return nil
	}
}

/// return a list of with the given key..
func (ls *libstore) GetList(key string) ([]string, error) {
	//	fmt.Println("GetList Called: ", key, ls.callback)
	/// check query status
	var wantlease bool
	if ls.mode == Always {
		//		fmt.Println("GETfunction: Always ask for lease.")
		wantlease = true
	} else if ls.mode == Never {
		//		fmt.Println("GETfunction: Nerver ask for lease.")
		wantlease = false
	} else {
		wantlease = ls.updateQueryCache(key)
	}

	if wantlease {
		//		fmt.Println("GETfunction: asking for lease.")
		ls.dMutex.Lock()
		defer ls.dMutex.Unlock()
	} else {
		//		fmt.Println("GETfunction: not asking for lease.")
	}

	// check data cache
	data, ok := ls.dataCache[key]
	if ok {
		if !ls.isExpired(data.expireTime) {
			// valid cache, return immediately
			//			fmt.Println("GetFunctionCalled: use cache..", key)
			return data.data.([]string), nil
		}
		//		fmt.Println("GetFunctionCalled: cache expired!!", key)
	}

	//	fmt.Println("GetFunctionCalled: no cache detected, grab key", key)

	args := &storagerpc.GetArgs{Key: key, WantLease: wantlease, HostPort: ls.callback}
	var reply storagerpc.GetListReply
	targetNode := ls.FindPartitionNode(ls.storages, key)
	if err := targetNode.conn.Call("StorageServer.GetList", args, &reply); err != nil {
		return nil, err
	}

	if reply.Status != storagerpc.OK {
		fmt.Println("Reply status is not OK.!")
		return nil, errors.New("Not fine")
	} else {
		// cache the data before return
		// note that the lease should alwasy be granted
		if reply.Lease.Granted != args.WantLease {
			//			fmt.Println("@@@@@@@@@@#### Get Function: Nasty Error:Why is the lease not granted???? Granted?", reply.Lease.Granted);
		} else if wantlease {
			ls.dataCache[key] = &dataUnit{expireTime: time.Now().UnixNano() + int64(reply.Lease.ValidSeconds)*int64(time.Second), data: reply.Value}
		}

		return reply.Value, nil
	}
}

/// remove the element from the list of the given key.
func (ls *libstore) RemoveFromList(key, removeItem string) error {
	//	fmt.Println("RemoveFromList Called: ", key, ls.callback)

	// check data cache
	//	ls.dMutex.Lock()
	//	_, ok := ls.dataCache[key]
	//	if ok {
	//		delete(ls.dataCache, key)
	//	}
	//	ls.dMutex.Unlock()

	args := &storagerpc.PutArgs{Key: key, Value: removeItem}
	var reply storagerpc.PutReply
	if err := ls.FindPartitionNode(ls.storages, key).conn.Call("StorageServer.RemoveFromList", args, &reply); err != nil {
		return err
	}

	if reply.Status != storagerpc.OK {
		return errors.New("Not fine")
	} else {
		return nil
	}
}

func (ls *libstore) AppendToList(key, newItem string) error {
	//	fmt.Println("AppendToList Called: ", key, ls.callback)

	// check data cache
	//	ls.dMutex.Lock()
	//	_, ok := ls.dataCache[key]
	//	if ok {
	//		delete(ls.dataCache, key)
	//	}
	//	ls.dMutex.Unlock()

	args := &storagerpc.PutArgs{Key: key, Value: newItem}
	var reply storagerpc.PutReply
	if err := ls.FindPartitionNode(ls.storages, key).conn.Call("StorageServer.AppendToList", args, &reply); err != nil {
		return err
	}

	if reply.Status != storagerpc.OK {
		return errors.New("Not fine")
	} else {
		return nil
	}
}

func (ls *libstore) RevokeLease(args *storagerpc.RevokeLeaseArgs, reply *storagerpc.RevokeLeaseReply) error {
	fmt.Println("RevodeLease Called:", args.Key, ls.callback)
	ls.dMutex.Lock()
	defer ls.dMutex.Unlock()

	_, ok := ls.dataCache[args.Key]
	if ok {
		reply.Status = storagerpc.OK
		delete(ls.dataCache, args.Key)
	} else {
		reply.Status = storagerpc.KeyNotFound
	}

	return nil
}
