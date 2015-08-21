package api

import (
	"fmt"
	"time"
)

type NodeId uint16
type StoreKey string
type NodeStatus uint8

const (
	NODE_STATUS_INVALID NodeStatus = iota
	NODE_STATUS_UP
	NODE_STATUS_DOWN
)

type NodeMetaInfo struct {
	Id           NodeId
	LastUpdateTs time.Time
}

type NodeInfo struct {
	Id           NodeId
	LastUpdateTs time.Time
	Status       NodeStatus
	Value        interface{}
}

func (n NodeInfo) String() string {
	return fmt.Sprintf("\nId: %v\nLastUpdateTs: %v\nStatus: : %v\nValue: %v",
		n.Id, n.LastUpdateTs, n.Status, n.Value)
}

type NodeInfoList struct {
	List []NodeInfo
}

type NodeMetaInfoList struct {
	List []NodeMetaInfo
}

// StoreValue is a map where the key is the
// StoreKey and the value is the NodeInfoList.
// This list gives the latest available view with this node
// for the whole system
type StoreValue map[StoreKey]NodeInfoList

// Used by the Gossip protocol
type StoreMetaInfo map[StoreKey]NodeMetaInfoList
type StoreDiff map[StoreKey]map[NodeId]NodeInfo
type StoreNodes map[StoreKey][]NodeId

type GossipStore interface {
	// NodeId of this Store
	NodeId() NodeId

	// Update updates the value for this node.
	// Side-effects include updating the last update ts
	// for this node.
	UpdateSelf(StoreKey, interface{})

	// GetStoreValue returns the StoreValue associated with
	// the given key
	GetStoreKeyValue(key StoreKey) NodeInfoList

	// GetStoreKeys returns all the keys present in the store
	GetStoreKeys() []StoreKey

	// Used for gossiping

	// Update updates the current state of the gossip data
	// with the newly available data
	Update(newData StoreDiff)

	// Subset returns the available gossip data for the given
	// nodes. Node data is returned if there is none available
	// for a given node
	Subset(nodes StoreNodes) StoreDiff

	// MetaInfoMap returns meta information for the
	// current available data
	MetaInfo() StoreMetaInfo

	// Diff returns a tuple of lists, where
	// first list is of the names of node for which
	// the current data is older as compared to the
	// given meta info, and second list is the names
	// of nodes for which the current data is newer
	Diff(d StoreMetaInfo) (StoreNodes, StoreNodes)
}

type Gossiper interface {
	// Gossiper has a gossip store
	GossipStore

	// SetGossipInterval sets the gossip interval
	SetGossipInterval(time.Duration)
	// GossipInterval gets the gossip interval
	GossipInterval() time.Duration

	// Stop stops the gossiping
	Stop()

	// AddNode adds a node to gossip with
	AddNode(ip string) error

	// RemoveNode removes the node to gossip with
	RemoveNode(ip string) error

	// GetNodes returns a list of the connection addresses
	// added via AddNode
	GetNodes() []string
}
