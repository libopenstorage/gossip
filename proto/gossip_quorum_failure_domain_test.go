package proto

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/libopenstorage/gossip/types"
	"github.com/stretchr/testify/require"
)

const (
	zone1 = "zone1"
	zone2 = "zone2"
	zone3 = "zone3"
)

var activeMap = types.MetroDomainsActiveMap{
	zone1: true,
	zone2: true,
	zone3: true,
}

func startNodeFd(
	t *testing.T,
	selfIp string,
	nodeId types.NodeId,
	selfFailureDomain string,
	knownNodesMap map[string]string,
	peers map[types.NodeId]types.NodeUpdate,
	activeMap types.MetroDomainsActiveMap,
) (*GossiperImpl, types.StoreKey) {
	g, _ := newGossiperImpl(
		selfIp,
		nodeId,
		selfFailureDomain,
		knownNodesMap,
		types.GOSSIP_VERSION_2,
		DEFAULT_CLUSTER_ID,
		types.QUORUM_PROVIDER_FAILURE_DOMAINS,
		activeMap,
	)
	g.UpdateCluster(peers)
	key := addKey(g)
	return g, key
}

func getIdFromAddr(addr string) (types.NodeId, int) {
	id := addr[len(addr)-1:]
	index, _ := strconv.ParseInt(id, 10, 64)
	return types.NodeId(id), int(index)
}

func setupTestNodes(t *testing.T) (map[string]string, map[types.NodeId]types.NodeUpdate, []*GossiperImpl) {
	nodes := map[string]string{
		"127.0.0.1:9300": zone1,
		"127.0.0.1:9301": zone1,
		"127.0.0.1:9302": zone2,
		"127.0.0.1:9303": zone2,
		"127.0.0.1:9304": zone3,
		"127.0.0.1:9305": zone3,
	}

	peers := map[types.NodeId]types.NodeUpdate{
		types.NodeId("0"): types.NodeUpdate{"127.0.0.1:9300", true, zone1},
		types.NodeId("1"): types.NodeUpdate{"127.0.0.1:9301", true, zone1},
		types.NodeId("2"): types.NodeUpdate{"127.0.0.1:9302", true, zone2},
		types.NodeId("3"): types.NodeUpdate{"127.0.0.1:9303", true, zone2},
		types.NodeId("4"): types.NodeUpdate{"127.0.0.1:9304", true, zone3},
		types.NodeId("5"): types.NodeUpdate{"127.0.0.1:9305", true, zone3},
	}

	gossipers := make([]*GossiperImpl, len(nodes))

	for nodeIp, failureDomain := range nodes {
		id, index := getIdFromAddr(nodeIp)
		g, _ := startNodeFd(t, nodeIp, id, failureDomain, nodes, peers, activeMap)
		gossipers[index] = g
	}

	// Let the nodes gossip to each other
	time.Sleep(gossipers[0].GossipInterval() * time.Duration(len(nodes)+1))

	for i, g := range gossipers {
		require.Equal(t, g.GetSelfStatus(), types.NODE_STATUS_UP, "Expected Node %v to be up", i)
	}

	return nodes, peers, gossipers
}

func shutdownTestNodes(gossipers []*GossiperImpl) {
	for _, g := range gossipers {
		err := g.Stop(time.Duration(0))
		fmt.Printf("Failed to stop gossiper for %v: %v \n", g.NodeId(), err)
	}
	time.Sleep(10 * time.Second)
}

// UTs for the Ping functionality

func TestQuorumFdOneNodeDown(t *testing.T) {
	printTestInfo()

	nodes, _, gossipers := setupTestNodes(t)

	// Let the nodes gossip to each other
	time.Sleep(gossipers[0].GossipInterval() * time.Duration(len(nodes)+1))

	for i, g := range gossipers {
		require.Equal(t, g.GetSelfStatus(), types.NODE_STATUS_UP, "Expected Node %v to be up", i)
	}

	downNodeId := gossipers[0].NodeId()
	// Stop Node 0
	gossipers[0].Stop(types.DEFAULT_GOSSIP_INTERVAL * time.Duration(len(nodes)+1))

	// Let the nodes gossip to each other
	time.Sleep(gossipers[0].GossipInterval() * time.Duration(4*len(nodes)+1))

	for id, g := range gossipers {
		if id == 0 {
			continue
		}
		peerDownNode, err := g.GetLocalNodeInfo(downNodeId)
		require.NoError(t, err, "Unexpected error on GetLocalNodeInfo")
		require.Equal(t, types.NODE_STATUS_DOWN, peerDownNode.Status, "Unexpected state found in %v for peer node %v", id, downNodeId)
	}

	shutdownTestNodes(gossipers)
}

func TestQuorumFdOneFailureDomainDownProbationExpires(t *testing.T) {
	printTestInfo()

	nodes, _, gossipers := setupTestNodes(t)

	// Stop all nodes in zone1
	downNodeId0 := gossipers[0].NodeId()
	downNodeId1 := gossipers[1].NodeId()

	go func() { gossipers[0].Stop(time.Duration(0)) }()
	go func() { gossipers[1].Stop(time.Duration(0)) }()

	// Let the nodes gossip to each other
	time.Sleep(gossipers[0].GossipInterval() * time.Duration(5*len(nodes)+1))

	// Both node 0 and 1 should be put in suspected down state and in the probation list
	for id, g := range gossipers {
		if id == 0 || id == 1 {
			continue
		}
		peerDownNode, err := g.GetLocalNodeInfo(downNodeId1)
		require.NoError(t, err, "Unexpected error on GetLocalNodeInfo")
		require.Equal(t, types.NODE_STATUS_SUSPECT_DOWN, peerDownNode.Status, "Unexpected state found in %v for peer node %v", id, downNodeId1)

		peerDownNode, err = g.GetLocalNodeInfo(downNodeId0)
		require.NoError(t, err, "Unexpected error on GetLocalNodeInfo")
		require.Equal(t, types.NODE_STATUS_SUSPECT_DOWN, peerDownNode.Status, "Unexpected state found in %v for peer node %v", id, downNodeId0)

	}

	// Wait for probation timeout to expire
	time.Sleep(suspectNodeDownTimeout)

	for id, g := range gossipers {
		if id == 0 || id == 1 {
			continue
		}
		peerDownNode, err := g.GetLocalNodeInfo(downNodeId1)
		require.NoError(t, err, "Unexpected error on GetLocalNodeInfo")
		require.Equal(t, types.NODE_STATUS_DOWN, peerDownNode.Status, "Unexpected state found in %v for peer node %v", id, downNodeId1)

		peerDownNode, err = g.GetLocalNodeInfo(downNodeId0)
		require.NoError(t, err, "Unexpected error on GetLocalNodeInfo")
		require.Equal(t, types.NODE_STATUS_DOWN, peerDownNode.Status, "Unexpected state found in %v for peer node %v", id, downNodeId0)

	}

	shutdownTestNodes(gossipers)
}

func TestQuorumFdOneFailureDomainDownAndBackUp(t *testing.T) {
	printTestInfo()

	nodes, peers, gossipers := setupTestNodes(t)

	// Stop all nodes in zone1
	downNodeId0 := gossipers[0].NodeId()
	downNodeId1 := gossipers[1].NodeId()

	go func() { gossipers[0].Stop(time.Duration(0)) }()
	go func() { gossipers[1].Stop(time.Duration(0)) }()

	// Let the nodes gossip to each other
	time.Sleep(gossipers[0].GossipInterval() * time.Duration(5*len(nodes)+1))

	// Both node 0 and 1 should be put in suspected down state and in the probation list
	for id, g := range gossipers {
		if id == 0 || id == 1 {
			continue
		}
		peerDownNode, err := g.GetLocalNodeInfo(downNodeId1)
		require.NoError(t, err, "Unexpected error on GetLocalNodeInfo")
		require.Equal(t, types.NODE_STATUS_SUSPECT_DOWN, peerDownNode.Status, "Unexpected state found in %v for peer node %v", id, downNodeId1)

		peerDownNode, err = g.GetLocalNodeInfo(downNodeId0)
		require.NoError(t, err, "Unexpected error on GetLocalNodeInfo")
		require.Equal(t, types.NODE_STATUS_SUSPECT_DOWN, peerDownNode.Status, "Unexpected state found in %v for peer node %v", id, downNodeId0)

	}

	// Sleep for less than node suspect timeout
	time.Sleep(5 * time.Second)

	for nodeIp, failureDomain := range nodes {
		id, index := getIdFromAddr(nodeIp)
		if id != downNodeId0 && id != downNodeId1 {
			continue
		}
		g, _ := startNodeFd(t, nodeIp, id, failureDomain, nodes, peers, activeMap)
		gossipers[index] = g
	}

	// Let the nodes gossip to each other
	time.Sleep(gossipers[0].GossipInterval() * time.Duration(1*len(nodes)+1))

	for id, g := range gossipers {
		peerDownNode, err := g.GetLocalNodeInfo(downNodeId1)
		require.NoError(t, err, "Unexpected error on GetLocalNodeInfo")
		require.Equal(t, types.NODE_STATUS_UP, peerDownNode.Status, "Unexpected state found in %v for peer node %v", id, downNodeId1)

		peerDownNode, err = g.GetLocalNodeInfo(downNodeId0)
		require.NoError(t, err, "Unexpected error on GetLocalNodeInfo")
		require.Equal(t, types.NODE_STATUS_UP, peerDownNode.Status, "Unexpected state found in %v for peer node %v", id, downNodeId0)

	}

	shutdownTestNodes(gossipers)
}

func testTwoFailureDomainsDown(t *testing.T) (map[string]string, map[types.NodeId]types.NodeUpdate, []*GossiperImpl, []types.NodeId) {
	nodes, peers, gossipers := setupTestNodes(t)

	// Stop all nodes in zone1 and zone2
	downNodes := []types.NodeId{}
	for i := 0; i < 4; i++ {
		gossipers[i].Stop(time.Duration(0))
		downNodes = append(downNodes, gossipers[i].NodeId())
	}

	// Let the nodes gossip to each other
	time.Sleep(gossipers[0].GossipInterval() * time.Duration(5*len(nodes)+1))

	// Nodes 0,1,2,3 should be in suspected offline
	for _, downNode := range downNodes {
		peerDownNode, err := gossipers[4].GetLocalNodeInfo(downNode)
		require.NoError(t, err, "Unexpected error on GetLocalNodeInfo")
		require.Equal(t, types.NODE_STATUS_SUSPECT_DOWN, peerDownNode.Status, "Unexpected state found in 4 for peer node %v", downNode)

		peerDownNode, err = gossipers[5].GetLocalNodeInfo(downNode)
		require.NoError(t, err, "Unexpected error on GetLocalNodeInfo")
		require.Equal(t, types.NODE_STATUS_SUSPECT_DOWN, peerDownNode.Status, "Unexpected state found in 5 for peer node %v", downNode)
	}

	// Nodes 4 and 5 should be suspected in quorum
	require.Equal(t, types.NODE_STATUS_SUSPECT_NOT_IN_QUORUM, gossipers[4].GetSelfStatus(), "Unexpected state found for node 4")
	require.Equal(t, types.NODE_STATUS_SUSPECT_NOT_IN_QUORUM, gossipers[5].GetSelfStatus(), "Unexpected state found for node 5")

	time.Sleep(suspectNodeDownTimeout)

	// Nodes 0,1,2,3 should be in offline state
	// Nodes 4 and 5 should  be Not in Quorum
	for _, downNode := range downNodes {
		peerDownNode, err := gossipers[4].GetLocalNodeInfo(downNode)
		require.NoError(t, err, "Unexpected error on GetLocalNodeInfo")
		require.Equal(t, types.NODE_STATUS_DOWN, peerDownNode.Status, "Unexpected state found in 4 for peer node %v", downNode)

		peerDownNode, err = gossipers[5].GetLocalNodeInfo(downNode)
		require.NoError(t, err, "Unexpected error on GetLocalNodeInfo")
		require.Equal(t, types.NODE_STATUS_DOWN, peerDownNode.Status, "Unexpected state found in 5 for peer node %v", downNode)
	}

	require.Equal(t, types.NODE_STATUS_NOT_IN_QUORUM, gossipers[4].GetSelfStatus(), "Unexpected state found for node 4")
	require.Equal(t, types.NODE_STATUS_NOT_IN_QUORUM, gossipers[5].GetSelfStatus(), "Unexpected state found for node 5")

	return nodes, peers, gossipers, downNodes
}

func TestQuorumFdTwoFailureDomainsDownOutOfQuorum(t *testing.T) {
	printTestInfo()

	_, _, gossipers, _ := testTwoFailureDomainsDown(t)

	shutdownTestNodes(gossipers)
}

func TestQuorumFdTwoFailureDomainsDownAndDeactivated(t *testing.T) {
	printTestInfo()

	_, _, gossipers, _ := testTwoFailureDomainsDown(t)

	// Update the deactivated list
	activeMap[zone1] = false
	activeMap[zone2] = false

	// Update the deactivated list
	err := gossipers[4].UpdateMetroDomainsActiveMap(activeMap)
	require.NoError(t, err, "Unexpected error on updating deactivated list for 4")
	err = gossipers[5].UpdateMetroDomainsActiveMap(activeMap)
	require.NoError(t, err, "Unexpected error on updating deactivated list for 5")

	time.Sleep(5 * time.Second)

	// Nodes 4 and 5 should be Online
	require.Equal(t, types.NODE_STATUS_UP, gossipers[4].GetSelfStatus(), "Unexpected state found for node 4")
	require.Equal(t, types.NODE_STATUS_UP, gossipers[5].GetSelfStatus(), "Unexpected state found for node 5")

	activeMap[zone1] = true
	activeMap[zone2] = true
	shutdownTestNodes(gossipers)
}

func TestQuorumFdTwoFailureDomainsDownDeactivatedAndReactivated(t *testing.T) {
	printTestInfo()

	nodes, peers, gossipers, _ := testTwoFailureDomainsDown(t)

	// Update the deactivated list
	activeMap[zone1] = false
	activeMap[zone2] = false

	err := gossipers[4].UpdateMetroDomainsActiveMap(activeMap)
	require.NoError(t, err, "Unexpected error on updating deactivated list for 4")
	err = gossipers[5].UpdateMetroDomainsActiveMap(activeMap)
	require.NoError(t, err, "Unexpected error on updating deactivated list for 5")

	time.Sleep(5 * time.Second)

	// Nodes 4 and 5 should be Online
	require.Equal(t, types.NODE_STATUS_UP, gossipers[4].GetSelfStatus(), "Unexpected state found for node 4")
	require.Equal(t, types.NODE_STATUS_UP, gossipers[5].GetSelfStatus(), "Unexpected state found for node 5")

	// Start node 0,1,2,3
	for nodeIp, failureDomain := range nodes {
		id, index := getIdFromAddr(nodeIp)
		if id == types.NodeId("4") || id == types.NodeId("5") {
			continue
		}
		g, _ := startNodeFd(t, nodeIp, id, failureDomain, nodes, peers, activeMap)
		gossipers[index] = g
	}

	// Let the nodes gossip to each other
	time.Sleep(gossipers[0].GossipInterval() * time.Duration(len(nodes)+1))

	// Nodes 0,1,2,3 should still be in Not In Quorum
	// Nodes 4 and 5 should be Online
	require.Equal(t, types.NODE_STATUS_NOT_IN_QUORUM, gossipers[0].GetSelfStatus(), "Unexpected state found for node 0")
	require.Equal(t, types.NODE_STATUS_NOT_IN_QUORUM, gossipers[1].GetSelfStatus(), "Unexpected state found for node 1")
	require.Equal(t, types.NODE_STATUS_NOT_IN_QUORUM, gossipers[2].GetSelfStatus(), "Unexpected state found for node 2")
	require.Equal(t, types.NODE_STATUS_NOT_IN_QUORUM, gossipers[3].GetSelfStatus(), "Unexpected state found for node 3")
	require.Equal(t, types.NODE_STATUS_UP, gossipers[5].GetSelfStatus(), "Unexpected state found for node 3")
	require.Equal(t, types.NODE_STATUS_UP, gossipers[5].GetSelfStatus(), "Unexpected state found for node 3")

	activeMap[zone2] = true
	// Remove zone 2 from deactivation list
	for _, g := range gossipers {
		g.UpdateMetroDomainsActiveMap(activeMap)
	}

	time.Sleep(5 * time.Second)

	require.Equal(t, types.NODE_STATUS_NOT_IN_QUORUM, gossipers[0].GetSelfStatus(), "Unexpected state found for node 0")
	require.Equal(t, types.NODE_STATUS_NOT_IN_QUORUM, gossipers[1].GetSelfStatus(), "Unexpected state found for node 1")
	require.Equal(t, types.NODE_STATUS_UP, gossipers[2].GetSelfStatus(), "Unexpected state found for node 2")
	require.Equal(t, types.NODE_STATUS_UP, gossipers[3].GetSelfStatus(), "Unexpected state found for node 3")
	require.Equal(t, types.NODE_STATUS_UP, gossipers[5].GetSelfStatus(), "Unexpected state found for node 4")
	require.Equal(t, types.NODE_STATUS_UP, gossipers[5].GetSelfStatus(), "Unexpected state found for node 5")

	activeMap[zone1] = true
	// Remove zone 1 from deactivation list
	for _, g := range gossipers {
		g.UpdateMetroDomainsActiveMap(activeMap)
	}

	time.Sleep(5 * time.Second)
	for i, g := range gossipers {
		require.Equal(t, types.NODE_STATUS_UP, g.GetSelfStatus(), "Unexpected state found for node %v", i)
	}

	shutdownTestNodes(gossipers)
}
