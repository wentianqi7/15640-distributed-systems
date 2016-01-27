// DO NOT MODIFY!

package main

import (
	"flag"
	"fmt"
	"log"
	"strings"

	"github.com/cmu440-F15/paxosapp/paxos"
)

var (
	ports      = flag.String("ports", "", "ports for all paxos nodes")
	numNodes   = flag.Int("N", 1, "the number of nodes in the ring")
	nodeID     = flag.Int("id", 0, "node ID must match index of this node's port in the ports list")
	numRetries = flag.Int("retries", 5, "number of times a node should retry dialing another node")
	proxyPort  = flag.Int("pxport", 9999, "(Staff use) port that the proxy server listens on")
	proxyNode  = flag.Int("proxy", -1, "(Staff use) node being monitored by the proxy")
)

func init() {
	log.SetFlags(log.Lshortfile | log.Lmicroseconds)
}

func main() {
	flag.Parse()

	portStrings := strings.Split(*ports, ",")

	hostMap := make(map[int]string)
	if *proxyNode >= 0 {
		if *proxyNode == *nodeID {
			// set up proxy routing
			for i, port := range portStrings {
				if i == *proxyNode {
					hostMap[*proxyNode] = "localhost:" + port
				} else {
					hostMap[i] = fmt.Sprintf("localhost:%d", *proxyPort)
				}
			}
		} else {
			for i, port := range portStrings {
				if i == *proxyNode {
					hostMap[*proxyNode] = fmt.Sprintf("localhost:%d", *proxyPort)
				} else {
					hostMap[i] = "localhost:" + port
				}
			}
		}
	} else {
		for i, port := range portStrings {
			hostMap[i] = "localhost:" + port
		}
	}

	// Create and start the Paxos Node.
	_, err := paxos.NewPaxosNode(hostMap[*nodeID], hostMap, *numNodes, *nodeID, *numRetries, false)
	if err != nil {
		log.Fatalln("Failed to create paxos node:", err)
	}

	// Run the paxos node forever.
	select {}
}
