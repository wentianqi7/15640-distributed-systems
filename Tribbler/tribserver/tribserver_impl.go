package tribserver

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/rpc/tribrpc"
	"github.com/cmu440/tribbler/util"
	"net"
	"net/http"
	"net/rpc"
	"sort"
	"strings"
	"time"
)

type tribServer struct {
	libstore libstore.Libstore
}

// NewTribServer creates, starts and returns a new TribServer. masterServerHostPort
// is the master storage server's host:port and port is this port number on which
// the TribServer should listen. A non-nil error should be returned if the TribServer
// could not be started.
//
// For hints on how to properly setup RPC, see the rpc/tribrpc package.
func NewTribServer(masterServerHostPort, myHostPort string) (TribServer, error) {

	tribServer := &tribServer{}

	var newError error
	tribServer.libstore, newError = libstore.NewLibstore(masterServerHostPort, myHostPort, libstore.Normal)
	if newError != nil {
		fmt.Println(newError)
	}

	err := rpc.RegisterName("TribServer", tribrpc.Wrap(tribServer))
	if err != nil {
		return nil, err
	}
	rpc.HandleHTTP()
	listener, err := net.Listen("tcp", ":"+strings.Split(myHostPort, ":")[1])
	if err != nil {
		return nil, err
	}

	go http.Serve(listener, nil)

	fmt.Println("TribServer Created!", myHostPort)
	return tribServer, nil
}

func (ts *tribServer) CreateUser(args *tribrpc.CreateUserArgs, reply *tribrpc.CreateUserReply) error {

	key := util.FormatUserKey(args.UserID)
	_, err := ts.libstore.Get(key)

	if err == nil {
		reply.Status = tribrpc.Exists
	} else {
		err2 := ts.libstore.Put(key, key)
		if err2 != nil {
			return err2
		}
		reply.Status = tribrpc.OK
	}

	return nil
}

// AddSubscription adds TargerUserID to UserID's list of subscriptions.
// Replies with status NoSuchUser if the specified UserID does not exist, and NoSuchTargerUser
// if the specified TargerUserID does not exist.
func (ts *tribServer) AddSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {

	userId := args.UserID
	targetId := args.TargetUserID

	userIdKey := util.FormatUserKey(userId)
	targetIdKey := util.FormatUserKey(targetId)

	_, ok_user := ts.libstore.Get(userIdKey)
	_, ok_target := ts.libstore.Get(targetIdKey)
	if ok_user != nil {
		reply.Status = tribrpc.NoSuchUser
	} else if ok_target != nil {
		reply.Status = tribrpc.NoSuchTargetUser
	} else {
		subKey := util.FormatSubListKey(userId)

		err := ts.libstore.AppendToList(subKey, targetId)
		if err != nil {
			reply.Status = tribrpc.Exists
		} else {
			reply.Status = tribrpc.OK
		}
	}
	return nil
}

func (ts *tribServer) RemoveSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {

	userId := args.UserID
	targetId := args.TargetUserID

	userIdKey := util.FormatUserKey(userId)
	targetIdKey := util.FormatUserKey(targetId)

	_, ok_user := ts.libstore.Get(userIdKey)
	_, ok_target := ts.libstore.Get(targetIdKey)
	if ok_user != nil {
		reply.Status = tribrpc.NoSuchUser
	} else if ok_target != nil {
		reply.Status = tribrpc.NoSuchTargetUser
	} else {
		subKey := util.FormatSubListKey(userId)

		err := ts.libstore.RemoveFromList(subKey, targetId)
		if err != nil {
			reply.Status = tribrpc.NoSuchTargetUser
		} else {
			reply.Status = tribrpc.OK
		}
	}

	return nil
}

func (ts *tribServer) GetSubscriptions(args *tribrpc.GetSubscriptionsArgs, reply *tribrpc.GetSubscriptionsReply) error {
	subkey := util.FormatSubListKey(args.UserID)
	userkey := util.FormatUserKey(args.UserID)
	_, userErr := ts.libstore.Get(userkey)
	if userErr != nil {
		reply.Status = tribrpc.NoSuchUser
	} else {
		list, _ := ts.libstore.GetList(subkey)
		reply.Status = tribrpc.OK
		reply.UserIDs = list
	}
	return nil
}

func (ts *tribServer) PostTribble(args *tribrpc.PostTribbleArgs, reply *tribrpc.PostTribbleReply) error {

	userkey := util.FormatUserKey(args.UserID)

	_, userErr := ts.libstore.Get(userkey)
	if userErr != nil {
		reply.Status = tribrpc.NoSuchUser
	} else {
		postTime := time.Now()
		tribble := &tribrpc.Tribble{
			UserID:   args.UserID,
			Posted:   postTime,
			Contents: args.Contents,
		}
		marshalTribble, _ := json.Marshal(tribble)

		unixTime := int64(postTime.UnixNano())
		postKey := util.FormatPostKey(args.UserID, unixTime)

		putErr := ts.libstore.Put(postKey, string(marshalTribble))
		if putErr != nil {
			return errors.New("Fail to put postkey and tribble")
		}

		tribListKey := util.FormatTribListKey(args.UserID)
		appendErr := ts.libstore.AppendToList(tribListKey, postKey)
		if appendErr != nil {
			return errors.New("Fail to append tribble to user")
		}
		reply.Status = tribrpc.OK
		reply.PostKey = postKey
	}

	return nil
}

func (ts *tribServer) DeleteTribble(args *tribrpc.DeleteTribbleArgs, reply *tribrpc.DeleteTribbleReply) error {
	userkey := util.FormatUserKey(args.UserID)
	_, userErr := ts.libstore.Get(userkey)
	if userErr != nil {
		reply.Status = tribrpc.NoSuchUser
	} else {

		delErr := ts.libstore.Delete(args.PostKey)
		if delErr != nil {
			reply.Status = tribrpc.NoSuchPost
			return nil
		}

		tribkey := util.FormatTribListKey(args.UserID)
		delListErr := ts.libstore.RemoveFromList(tribkey, args.PostKey)
		if delListErr != nil {
			reply.Status = tribrpc.NoSuchPost
			return nil
		}

		reply.Status = tribrpc.OK

	}
	return nil
}

func (ts *tribServer) GetTribblesOneUser(userId string) []tribrpc.Tribble {
	tribkey := util.FormatTribListKey(userId)
	tribIdList, err := ts.libstore.GetList(tribkey)
	if err != nil {
		return nil
	}

	var tribble tribrpc.Tribble
	length := len(tribIdList)
	if length > 100 {
		length = 100
	}
	tribList := make([]tribrpc.Tribble, 0, length)
	i := 0
	for len(tribList) < length && len(tribIdList) >= 1+i {
		tribId := tribIdList[len(tribIdList)-1-i]
		tribbleString, err := ts.libstore.Get(tribId)
		if err == nil {
			json.Unmarshal([]byte(tribbleString), &tribble)
			tribList = append(tribList, tribble)
		}
		i++
	}

	return tribList
}

func (ts *tribServer) GetTribbles(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {

	userkey := util.FormatUserKey(args.UserID)
	_, userErr := ts.libstore.Get(userkey)
	if userErr != nil {
		reply.Status = tribrpc.NoSuchUser
	} else {
		reply.Status = tribrpc.OK
		tribList := ts.GetTribblesOneUser(args.UserID)

		if tribList != nil {
			length := len(tribList)

			reply.Tribbles = make([]tribrpc.Tribble, length)
			for i := 0; i < length; i++ {
				reply.Tribbles[i] = tribList[i]
			}

		}
	}

	return nil
}

type Tribbles []tribrpc.Tribble

func (tribbles Tribbles) Len() int {
	return len(tribbles)
}

func (tribbles Tribbles) Less(i, j int) bool {
	return tribbles[i].Posted.After(tribbles[j].Posted)
}

func (tribbles Tribbles) Swap(i, j int) {
	tribbles[i], tribbles[j] = tribbles[j], tribbles[i]
}

func (ts *tribServer) GetTribblesBySubscription(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	userkey := util.FormatUserKey(args.UserID)
	_, userErr := ts.libstore.Get(userkey)

	if userErr != nil {
		reply.Status = tribrpc.NoSuchUser
	} else {
		reply.Status = tribrpc.OK
		subskey := util.FormatSubListKey(args.UserID)
		subsList, _ := ts.libstore.GetList(subskey)

		totalTribList := make(Tribbles, 0)
		for i := 0; i < len(subsList); i++ {
			curUser := subsList[i]
			curtribList := ts.GetTribblesOneUser(curUser)

			for j := 0; j < len(curtribList); j++ {
				totalTribList = append(totalTribList, curtribList[j])
			}
			//totalTribList = append(totalTribList, curtribList)
		}

		sort.Sort(totalTribList)
		totalLen := len(totalTribList)
		if totalLen > 100 {
			totalLen = 100
		}
		reply.Tribbles = make(Tribbles, totalLen)
		for j := 0; j < totalLen; j++ {
			reply.Tribbles[j] = totalTribList[j]
		}
	}
	return nil

}
