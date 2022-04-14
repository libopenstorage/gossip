# gossip

[![Travis branch](https://img.shields.io/travis/libopenstorage/gossip/master.svg)](https://travis-ci.org/libopenstorage/gossip)
[![Code Coverage](https://codecov.io/gh/libopenstorage/gossip/branch/master/graph/badge.svg)](https://codecov.io/gh/libopenstorage/gossip)


Go implementation of the Gossip protocol.

## Overview

This package provides an implementation of an eventually consistent in-memory
data store. The data store values are exchanged using a push-pull gossip protocol.

```
// Create a gossiper
g := NewGossiper("<ip>:<port>", "<unique node id>", "<peer-list>")
// Add peer nodes with whom you want to gossip
g.AddNode("<peer_ip>:<peer_port>")
...
// update self values
g.UpdateSelf("<some_key>", "<any_value>")
// start gossip
g.Start()
```

These values are exchanged using the gossip protocol between the configured
peers.

```
// Get the current view of the world
storeKeys = g.GetStoreKeys()
for _, key := range storeKeys.List {
	nodeInfoMap := g.GetStoreKeyValue(key)
	for id,  nodeInfo := nodeInfoList.List {
		// if nodeInfo.Status != types.NODE_STATUS_INVALID
        // then nodeInfo has valid data.
	}
}

// Stop gossiping
g.Stop()
```

## Contributing

### Testing

To test, run `make test`. This will run the unit tests

### Vendoring

For vendoring, use `make vendor` and `make vendor-update`. This project uses Go Modules.
