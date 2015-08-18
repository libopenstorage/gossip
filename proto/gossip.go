package proto

import (
	"bytes"
	"encoding/json"
	"errors"
	log "github.com/Sirupsen/logrus"
	"math/rand"
	"net"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	// XXX/gsangle: Should these come from some sort of config file?
	CONN_HOST = "0.0.0.0"
	CONN_PORT = "9002"
	CONN_TYPE = "tcp"

	// interval to gossip, may be should make it configurable ?
	GOSSIP_INTERVAL = 2 * time.Minute
)

func connectionString(ip string) string {
	if strings.Index(ip, ":") == -1 {
		return ip + ":" + CONN_PORT
	}
	return ip
}

// Implements the UnreliableBroadcast interface
type Gossip struct {
	// node list, maintained separately
	nodes     []string
	name      string
	id        NodeId
	nodesLock sync.Mutex
	// to signal exit gossip loop
	done chan bool

	// the actual in-memory state
	store GossipStore
}

// Utility methods
func logAndGetError(msg string) error {
	log.Error(msg)
	return errors.New(msg)
}

// New returns an initialized Gossip node
// which identifies itself with the given ip
func NewGossip(ip string) *Gossip {
	gs := NewGossipStore()
	return new(Gossip).init(ip, gs)
}

func (g *Gossip) init(ip string, gs GossipStore) *Gossip {
	g.name = ip
	g.nodes = make([]string, 10) // random initial capacity
	g.store = gs
	g.done = make(chan bool, 1)
	rand.Seed(time.Now().UnixNano())
	err := g.AddNode(ip)
	if err != nil {
		log.Error("Failed to add init node to store")
		return nil
	}
	return g
}

func (g *Gossip) Done() {
	g.done <- true
}

func (g *Gossip) AddNode(ip string) error {
	g.nodesLock.Lock()
	defer g.nodesLock.Unlock()

	for _, node := range g.nodes {
		if node == ip {
			return logAndGetError("Node being added already exists:" + ip)
		}
	}
	g.nodes = append(g.nodes, ip)

	return nil
}

func (g *Gossip) RemoveNode(ip string) error {
	g.nodesLock.Lock()
	defer g.nodesLock.Unlock()

	for i, node := range g.nodes {
		if node == ip {
			// not sure if this is the most efficient way
			g.nodes = append(g.nodes[:i], g.nodes[i+1:]...)
			return nil
		}
	}
	return logAndGetError("Node being added already exists:" + ip)
}

func (g *Gossip) GetNodes() []string {
	g.nodesLock.Lock()
	defer g.nodesLock.Unlock()

	nodeList := make([]string, len(g.nodes))
	copy(nodeList, g.nodes)
	return nodeList
}

// sendData serializes the given object and sends
// it over the given connection. Returns nil if
// it was successful, error otherwise
func sendData(obj interface{}, conn net.Conn) error {
	err := error(nil)
	buf, err := json.Marshal(obj)
	if err != nil {
		log.Error("Failed to serialize message", err)
		return err
	}

	for len(buf) > 0 {
		l, err := conn.Write(buf)
		if err != nil && err != syscall.EINTR {
			log.Error("Write failed: ", err)
			return err
		}
		buf = buf[l:]
	}

	return nil
}

// rcvData receives bytes over the connection
// until it can marshal the object. msg is the
// pointer to the object which will receive the data.
// Returns nil if it was successful, error otherwise.
func rcvData(msg interface{}, conn net.Conn) error {

	msgBuffer := new(bytes.Buffer)

	for {
		// XXX FIXME: What if the other node sends crap ?
		// this may never exit in such case
		_, err := msgBuffer.ReadFrom(conn)
		if err != nil && err != syscall.EINTR {
			log.Error("Error reading data from peer:", err)
			return err
		}

		err = json.Unmarshal(msgBuffer.Bytes(), msg)
		if err != nil {
			log.Warn("Received bad packet:", err)
			return err
		} else {
			return nil
		}
	}

	return nil
}

// getUpdatesFromPeer receives node data from the peer
// for which the peer has more latest information available
func (g *Gossip) getUpdatesFromPeer(conn net.Conn) error {

	var newPeerData StoreValueMap
	err := rcvData(&newPeerData, conn)
	if err != nil {
		log.Error("Error fetching the latest peer data", err)
		return err
	}

	g.store.Update(newPeerData)

	return nil
}

// sendNodeMetaInfo sends a list of meta info for all
// the nodes in the nodes's store to the peer
func (g *Gossip) sendNodeMetaInfo(conn net.Conn) error {
	msg := g.store.MetaInfo()
	err := sendData(msg, conn)
	return err
}

// sendUpdatesToPeer sends the information about the given
// nodes to the peer
func (g *Gossip) sendUpdatesToPeer(diff *StoreValueIdInfoMap, conn net.Conn) error {
	dataToSend := g.store.Subset(*diff)
	return sendData(dataToSend, conn)
}

func (g *Gossip) handleGossip(conn net.Conn) {
	var peerMetaInfoList StoreValueMetaInfoMap
	err := error(nil)

	// 1. Get the info about the node data that the sender has
	// XXX FIXME : readPeerData must be passed using a pointer
	err = rcvData(&peerMetaInfoList, conn)
	if err != nil {
		return
	}

	// 2. Compare with current data that this node has and get
	//    the names of the nodes for which this node has stale info
	//    as compared to the sender
	diffNew, selfNew := g.store.Diff(peerMetaInfoList)

	// 3. Send this list to the peer, and get the latest data
	// for them
	err = sendData(diffNew, conn)
	if err != nil {
		log.Error("Error sending list of nodes to fetch: ", err)
		return
	}

	// 4. get the data for nodes sent above from the peer
	err = g.getUpdatesFromPeer(conn)
	if err != nil {
		log.Error("Failed to get data for nodes from the peer: ", err)
		return
	}

	// 4. Since you know which data is stale on the sender side,
	//    send him the data for the updated nodes
	err = g.sendUpdatesToPeer(&selfNew, conn)
	if err != nil {
		return
	}
}

func (g *Gossip) receive_loop() {
	l, err := net.Listen(CONN_TYPE, CONN_HOST+":"+CONN_PORT)
	if err != nil {
		log.Println("Error listening:", err.Error())
		os.Exit(1)
	}
	defer l.Close()

	log.Println("Listening on " + CONN_HOST + ":" + CONN_PORT)

	for {
		conn, err := l.Accept()
		if err != nil {
			log.Println("Error accepting: ", err.Error())
			os.Exit(1)
		}
		// Handle connections in a new goroutine.
		go g.handleGossip(conn)
	}
}

// send_loop periodically connects to a random peer
// and gossips about the state of the cluster
func (g *Gossip) send_loop() {
	tick := time.Tick(GOSSIP_INTERVAL)
	for {
		select {
		case <-tick:
			g.gossip()
		case <-g.done:
			log.Info("send_loop now exiting")
		default:
			log.Error("send_loop default!")
		}
	}

}

// selectGossipPeer randomly selects a peer
// to gossip with from the list of nodes added
// XXX/gsangle : should we add discovered nodes
// from gossip data to this list of nodes as well ?
func (g *Gossip) selectGossipPeer() string {
	g.nodesLock.Lock()
	defer g.nodesLock.Unlock()

	nodesLen := len(g.nodes)
	if nodesLen != 0 {
		log.Info("No peers to gossip with, returning")
		return ""
	}

	return g.nodes[rand.Intn(nodesLen)]
}

func (g *Gossip) gossip() {

	// select a node to gossip with
	peerNode := g.selectGossipPeer()
	if len(peerNode) == 0 {
		return
	}

	conn, err := net.Dial(CONN_TYPE, connectionString(peerNode))
	if err != nil {
		log.Error("Peer " + peerNode + " unavailable to gossip")
		//XXX: FIXME : note that the peer is down
		return
	}

	// send meta data info about the node to the peer
	err = g.sendNodeMetaInfo(conn)
	if err != nil {
		log.Error("Failed to send meta info to the peer: ", err)
		//XXX: FIXME : note that the peer is down
		return
	}

	// get a list of requested nodes from the peer and
	var diff StoreValueIdInfoMap
	err = rcvData(&diff, conn)
	if err != nil {
		log.Error("Failed to get request info to the peer: ", err)
		//XXX: FIXME : note that the peer is down
		return
	}

	// send back the data
	err = g.sendUpdatesToPeer(&diff, conn)
	if err != nil {
		log.Error("Failed to send newer data to the peer: ", err)
		//XXX: FIXME : note that the peer is down
		return
	}

	// receive any updates the send has for us
	err = g.getUpdatesFromPeer(conn)
	if err != nil {
		log.Error("Failed to get newer data from the peer: ", err)
		//XXX: FIXME : note that the peer is down
		return
	}

}
