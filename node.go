package asg3

import (
	"log"
)

// The main participant of the distributed snapshot protocol.
// nodes exchange token messages and marker messages among each other.
// Token messages represent the transfer of tokens from one node to another.
// Marker messages represent the progress of the snapshot process. The bulk of
// the distributed protocol is implemented in `HandlePacket` and `StartSnapshot`.

type Node struct {
	sim           *ChandyLamportSim
	id            string
	tokens        int
	outboundLinks map[string]*Link // key = link.dest
	inboundLinks  map[string]*Link // key = link.src

	// TODO: add more fields here (what does each node need to keep track of?)
	// the key is the snapshot ID for all of these
	recievedMarkers  map[int]int
	recievedMessages map[int][]MsgSnapshot
	markedChannels   map[int][]string
	started          map[int]bool
	finished         map[int]bool
	initiatingNode   map[int]bool
	snapshotTime     map[int]int
	snapshotValue    map[int]int
}

// A unidirectional communication channel between two nodes
// Each link contains an event queue (as opposed to a packet queue)
type Link struct {
	src      string
	dest     string
	msgQueue *Queue
}

func CreateNode(id string, tokens int, sim *ChandyLamportSim) *Node {
	return &Node{
		sim:           sim,
		id:            id,
		tokens:        tokens,
		outboundLinks: make(map[string]*Link),
		inboundLinks:  make(map[string]*Link),

		// TODO: You may need to modify this if you make modifications above
		recievedMarkers:  make(map[int]int),
		recievedMessages: make(map[int][]MsgSnapshot),
		markedChannels:   make(map[int][]string),
		started:          make(map[int]bool),
		finished:         make(map[int]bool),
		initiatingNode:   make(map[int]bool),
		snapshotTime:     make(map[int]int),
		snapshotValue:    make(map[int]int),
	}
}

// Add a unidirectional link to the destination node
func (node *Node) AddOutboundLink(dest *Node) {
	if node == dest {
		return
	}
	l := Link{node.id, dest.id, NewQueue()}
	node.outboundLinks[dest.id] = &l
	dest.inboundLinks[node.id] = &l
}

// Send a message on all of the node's outbound links
func (node *Node) SendToNeighbors(message Message) {
	for _, nodeId := range getSortedKeys(node.outboundLinks) {
		link := node.outboundLinks[nodeId]
		node.sim.logger.RecordEvent(
			node,
			SentMsgRecord{node.id, link.dest, message})
		link.msgQueue.Push(SendMsgEvent{
			node.id,
			link.dest,
			message,
			node.sim.GetReceiveTime()})
	}
}

// Send a number of tokens to a neighbor attached to this node
func (node *Node) SendTokens(numTokens int, dest string) {
	if node.tokens < numTokens {
		log.Fatalf("node %v attempted to send %v tokens when it only has %v\n",
			node.id, numTokens, node.tokens)
	}
	message := Message{isMarker: false, data: numTokens}
	node.sim.logger.RecordEvent(node, SentMsgRecord{node.id, dest, message})
	// Update local state before sending the tokens
	node.tokens -= numTokens
	link, ok := node.outboundLinks[dest]
	if !ok {
		log.Fatalf("Unknown dest ID %v from node %v\n", dest, node.id)
	}

	link.msgQueue.Push(SendMsgEvent{
		node.id,
		dest,
		message,
		node.sim.GetReceiveTime()})
}

func (node *Node) HandlePacket(src string, message Message) {
	// TODO: Write this method
	// lock the resources
	node.sim.mutex.Lock()
	defer node.sim.mutex.Unlock()
	if message.isMarker {
		snapshotId := message.data
		// increment the marker
		node.recievedMarkers[snapshotId]++
		// if first marker recieved
		if node.recievedMarkers[snapshotId] == 1 && !node.initiatingNode[snapshotId] {
			// take a snapshot
			node.StartSnapshot(snapshotId)
		}
		node.markedChannels[snapshotId] = append(node.markedChannels[snapshotId], src)
		if node.recievedMarkers[snapshotId] == len(node.inboundLinks) {
			// all nodes recieved a marker
			node.sim.NotifyCompletedSnapshot(node.id, snapshotId)
		}
	} else {
		// record messages for each snapshot
		for i := 0; i < node.sim.nextSnapshotId; i++ {
			if node.started[i] && !node.finished[i] && node.snapshotTime[i] <= node.sim.time && !contains(node.markedChannels[i], src) {
				node.recievedMessages[i] = append(node.recievedMessages[i], MsgSnapshot{src, node.id, message})
			}
		}
		// update node tokens
		node.tokens += message.data
	}
}

func (node *Node) StartSnapshot(snapshotId int) {
	// TODO: Write this method
	// initialize the variables
	node.started[snapshotId] = true
	node.finished[snapshotId] = false
	node.snapshotTime[snapshotId] = node.sim.time
	node.snapshotValue[snapshotId] = node.tokens
	// send a marker to each outbound channel
	node.SendToNeighbors(Message{isMarker: true, data: snapshotId})
}

func contains(slice []string, val string) bool {
	for _, x := range slice {
		if x == val {
			return true
		}
	}
	return false
}
