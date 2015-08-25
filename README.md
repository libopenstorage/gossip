# gossip
Go implementation of the Gossip protocol.

This package provides an implementation of an eventually consistent in-memory
data store. The data store values are exchanged using a push-pull gossip protocol.

// Create a gossiper
g := NewGossiper("<ip>:<port>", <unique node id>)
// Add peer nodes with whom you want to gossip
g.AddNode("<peer_node>")
...
// update self values 
g.UpdateSelfValue("<some_key>", "<any_value>")

These values are exchanged using the gossip protocol between the configured
peers.

// Get the current view of the world
store_keys = g.GetStoreKeys()
for _, key := range store_keys {
	node_info_list := g.GetStoreKeyValue(key)
	// node_info_list is an array, to be indexed
	// by node id. Valid nodes can be identified
	// by the following:
	//    node_info.Status != api.NODE_STATUS_INVALID
}

// Stop gossiping
g.Stop()
