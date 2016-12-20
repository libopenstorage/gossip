package proto

import (
	"github.com/libopenstorage/gossip/types"
	"strconv"
	"testing"
	"time"
)

const DEFAULT_CLUSTER_ID = "test-cluster"

func addKey(g *GossiperImpl) types.StoreKey {
	key := types.StoreKey("new_key")
	value := "new_value"
	g.UpdateSelf(key, value)
	return key
}

func startNode(t *testing.T, selfIp string, nodeId types.NodeId, peerIps []string, peers map[types.NodeId]string) (*GossiperImpl, types.StoreKey) {
	g, _ := NewGossiperImpl(selfIp, nodeId, peerIps, types.DEFAULT_GOSSIP_VERSION)
	g.UpdateCluster(peers)
	key := addKey(g)
	return g, key
}

func TestQuorumAllNodesUpOneByOne(t *testing.T) {
	printTestInfo()

	nodes := []string{
		"127.0.0.1:9900",
		"127.0.0.2:9901",
	}

	// Start Node0 with cluster size 1
	node0 := types.NodeId("0")
	g0, _ := startNode(t, nodes[0], node0, []string{}, map[types.NodeId]string{node0: nodes[0]})

	if g0.GetSelfStatus() != types.NODE_STATUS_UP {
		t.Error("Expected Node 0 to have status: ", types.NODE_STATUS_UP)
	}

	// Start Node1 with cluster size 2
	node1 := types.NodeId("1")
	peers := map[types.NodeId]string{node0: nodes[0], node1: nodes[1]}
	g1, _ := startNode(t, nodes[1], node1, []string{nodes[0]}, peers)
	g0.UpdateCluster(peers)

	time.Sleep(g1.GossipInterval() * time.Duration(len(nodes)+1))

	if g1.GetSelfStatus() != types.NODE_STATUS_UP {
		t.Error("Expected Node 1 to have status: ", types.NODE_STATUS_UP)
	}

	// Check if Node0 is still Up
	if g0.GetSelfStatus() != types.NODE_STATUS_UP {
		t.Error("Expected Node 0 to have status: ", types.NODE_STATUS_UP)
	}

	g0.Stop(types.DEFAULT_GOSSIP_INTERVAL * time.Duration(len(nodes)+1))
	g1.Stop(types.DEFAULT_GOSSIP_INTERVAL * time.Duration(len(nodes)+1))
}

func TestQuorumNodeLoosesQuorumAndGainsBack(t *testing.T) {
	printTestInfo()

	nodes := []string{
		"127.0.0.1:9902",
		"127.0.0.2:9903",
	}

	node0 := types.NodeId("0")
	node1 := types.NodeId("1")
	// Start Node 0
	g0, _ := startNode(t, nodes[0], node0, []string{}, map[types.NodeId]string{node0: nodes[0]})

	selfStatus := g0.GetSelfStatus()
	if selfStatus != types.NODE_STATUS_UP {
		t.Error("Expected Node 0 to have status: ", types.NODE_STATUS_UP,
			" Got: ", selfStatus)
	}

	// Simulate new node was added by updating the cluster size, but the new node is not talking to node0
	// Node 0 should loose quorom 1/2
	g0.UpdateCluster(map[types.NodeId]string{node0: nodes[0], node1: nodes[1]})
	selfStatus = g0.GetSelfStatus()
	if selfStatus != types.NODE_STATUS_SUSPECT_NOT_IN_QUORUM {
		t.Error("Expected Node 0 to have status: ", types.NODE_STATUS_SUSPECT_NOT_IN_QUORUM,
			" Got: ", selfStatus)
	}

	// Sleep for quorum timeout
	time.Sleep(g0.quorumTimeout + 2*time.Second)

	selfStatus = g0.GetSelfStatus()
	if selfStatus != types.NODE_STATUS_NOT_IN_QUORUM {
		t.Error("Expected Node 0 to have status: ", types.NODE_STATUS_NOT_IN_QUORUM,
			" Got: ", selfStatus)
	}

	// Lets start the actual Node 1
	g1, _ := startNode(t, nodes[1], node1, []string{nodes[0]}, map[types.NodeId]string{node0: nodes[0], node1: nodes[1]})

	// Sleep so that nodes gossip
	time.Sleep(g1.GossipInterval() * time.Duration(len(nodes)+1))

	selfStatus = g0.GetSelfStatus()
	if selfStatus != types.NODE_STATUS_UP {
		t.Error("Expected Node 0 to have status: ", types.NODE_STATUS_UP,
			" Got: ", selfStatus)
	}
	selfStatus = g1.GetSelfStatus()
	if selfStatus != types.NODE_STATUS_UP {
		t.Error("Expected Node 1 to have status: ", types.NODE_STATUS_UP,
			" Got: ", selfStatus)
	}
}

func TestQuorumTwoNodesLooseConnectivity(t *testing.T) {
	printTestInfo()

	nodes := []string{
		"127.0.0.1:9904",
		"127.0.0.2:9905",
	}

	node0 := types.NodeId("0")
	node1 := types.NodeId("1")
	g0, _ := startNode(t, nodes[0], node0, []string{}, map[types.NodeId]string{node0: nodes[0]})

	if g0.GetSelfStatus() != types.NODE_STATUS_UP {
		t.Error("Expected Node 0 to have status: ", types.NODE_STATUS_UP)
	}

	// Simulate new node was added by updating the cluster size, but the new node is not talking to node0
	// Node 0 should loose quorom 1/2
	g0.UpdateCluster(map[types.NodeId]string{node0: nodes[0], node1: nodes[1]})
	if g0.GetSelfStatus() != types.NODE_STATUS_SUSPECT_NOT_IN_QUORUM {
		t.Error("Expected Node 0 to have status: ", types.NODE_STATUS_SUSPECT_NOT_IN_QUORUM)
	}

	// Lets start the actual node 1. We do not supply node 0 Ip address here so that node 1 does not talk to node 0
	// to simulate NO connectivity between node 0 and node 1
	g1, _ := startNode(t, nodes[1], node1, []string{}, map[types.NodeId]string{node0: nodes[0], node1: nodes[1]})

	// For node 0 the status will change from UP_WAITING_QUORUM to WAITING_QUORUM after
	// the quorum timeout
	time.Sleep(g0.quorumTimeout + 5*time.Second)

	if g0.GetSelfStatus() != types.NODE_STATUS_NOT_IN_QUORUM {
		t.Error("Expected Node 0 to have status: ", types.NODE_STATUS_NOT_IN_QUORUM, " Got: ", g0.GetSelfStatus())
	}
	if g1.GetSelfStatus() != types.NODE_STATUS_NOT_IN_QUORUM {
		t.Error("Expected Node 1 to have status: ", types.NODE_STATUS_NOT_IN_QUORUM, "Got: ", g1.GetSelfStatus())
	}
}

func TestQuorumOneNodeIsolated(t *testing.T) {
	printTestInfo()

	nodes := []string{
		"127.0.0.1:9906",
		"127.0.0.2:9907",
		"127.0.0.3:9908",
	}

	peers := make(map[types.NodeId]string)
	for i, ip := range nodes {
		nodeId := types.NodeId(strconv.FormatInt(int64(i), 10))
		peers[nodeId] = ip
	}

	var gossipers []*GossiperImpl
	for i, ip := range nodes {
		nodeId := types.NodeId(strconv.FormatInt(int64(i), 10))
		var g *GossiperImpl
		if i == 0 {
			g, _ = startNode(t, ip, nodeId, []string{}, peers)
		} else {
			g, _ = startNode(t, ip, nodeId, []string{nodes[0]}, peers)
		}

		gossipers = append(gossipers, g)
	}

	// Lets sleep so that the nodes gossip and update their quorum
	time.Sleep(types.DEFAULT_GOSSIP_INTERVAL * time.Duration(len(nodes)+1))

	for i, g := range gossipers {
		if g.GetSelfStatus() != types.NODE_STATUS_UP {
			t.Error("Expected Node ", i, " status to be ", types.NODE_STATUS_UP, " Got: ", g.GetSelfStatus())
		}
	}

	// Isolate node 1
	// Simulate isolation by stopping gossiper for node 1 and starting it back,
	// but by not providing peer IPs and setting cluster size to 3.
	gossipers[1].Stop(time.Duration(10) * time.Second)
	gossipers[1].InitStore(types.NodeId("1"), "v1", types.NODE_STATUS_NOT_IN_QUORUM, DEFAULT_CLUSTER_ID)
	gossipers[1].Start([]string{})

	// Lets sleep so that the nodes gossip and update their quorum
	time.Sleep(types.DEFAULT_GOSSIP_INTERVAL * time.Duration(len(nodes)+1))

	for i, g := range gossipers {
		if i == 1 {
			if g.GetSelfStatus() != types.NODE_STATUS_NOT_IN_QUORUM {
				t.Error("Expected Node ", i, " status to be ", types.NODE_STATUS_NOT_IN_QUORUM, " Got: ", g.GetSelfStatus())
			}
			continue
		}
		if g.GetSelfStatus() != types.NODE_STATUS_UP {
			t.Error("Expected Node ", i, " status to be ", types.NODE_STATUS_UP, " Got: ", g.GetSelfStatus())
		}
	}
}

func TestQuorumNetworkPartition(t *testing.T) {
	printTestInfo()
	nodes := []string{
		"127.0.0.1:9909",
		"127.0.0.2:9910",
		"127.0.0.3:9911",
		"127.0.0.4:9912",
		"127.0.0.5:9913",
	}

	// Simulate a network parition. Node 0-2 in parition 1. Node 3-4 in partition 2.
	var gossipers []*GossiperImpl
	// Partition 1
	for i := 0; i < 3; i++ {
		nodeId := types.NodeId(strconv.FormatInt(int64(i), 10))
		var g *GossiperImpl
		g, _ = startNode(t, nodes[i], nodeId, []string{nodes[0], nodes[1], nodes[2]}, map[types.NodeId]string{nodeId: nodes[i]})
		gossipers = append(gossipers, g)
	}
	// Parition 2
	for i := 3; i < 5; i++ {
		nodeId := types.NodeId(strconv.FormatInt(int64(i), 10))
		var g *GossiperImpl
		g, _ = startNode(t, nodes[i], nodeId, []string{nodes[3], nodes[4]}, map[types.NodeId]string{nodeId: nodes[i]})
		gossipers = append(gossipers, g)
	}
	// Let the nodes gossip
	time.Sleep(types.DEFAULT_GOSSIP_INTERVAL * time.Duration(len(nodes)))
	for i, g := range gossipers {
		if g.GetSelfStatus() != types.NODE_STATUS_UP {
			t.Error("Expected Node ", i, " status to be ", types.NODE_STATUS_UP, " Got: ", g.GetSelfStatus())
		}
	}

	peers := make(map[types.NodeId]string)
	for i, ip := range nodes {
		nodeId := types.NodeId(strconv.FormatInt(int64(i), 10))
		peers[nodeId] = ip
	}
	// Setup the partition by updating the cluster size
	for _, g := range gossipers {
		g.UpdateCluster(peers)
	}

	// Let the nodes update their quorum
	time.Sleep(time.Duration(3) * time.Second)
	// Partition 1
	for i := 0; i < 3; i++ {
		if gossipers[i].GetSelfStatus() != types.NODE_STATUS_UP {
			t.Error("Expected Node ", i, " status to be ", types.NODE_STATUS_UP, " Got: ", gossipers[i].GetSelfStatus())
		}

	}
	// Parition 2
	for i := 3; i < 5; i++ {
		if gossipers[i].GetSelfStatus() != types.NODE_STATUS_SUSPECT_NOT_IN_QUORUM {
			t.Error("Expected Node ", i, " status to be ", types.NODE_STATUS_SUSPECT_NOT_IN_QUORUM, " Got: ", gossipers[i].GetSelfStatus())
		}
	}

	time.Sleep(TestQuorumTimeout)
	// Parition 2
	for i := 3; i < 5; i++ {
		if gossipers[i].GetSelfStatus() != types.NODE_STATUS_NOT_IN_QUORUM {
			t.Error("Expected Node ", i, " status to be ", types.NODE_STATUS_NOT_IN_QUORUM, " Got: ", gossipers[i].GetSelfStatus())
		}
	}
}

func TestQuorumEventHandling(t *testing.T) {
	printTestInfo()

	nodes := []string{
		"127.0.0.1:9914",
		"127.0.0.2:9915",
		"127.0.0.3:9916",
		"127.0.0.4:9917",
		"127.0.0.5:9918",
	}

	// Start all nodes
	var gossipers []*GossiperImpl
	for i := 0; i < len(nodes); i++ {
		nodeId := types.NodeId(strconv.FormatInt(int64(i), 10))
		var g *GossiperImpl
		g, _ = startNode(t, nodes[i], nodeId, []string{nodes[0]}, map[types.NodeId]string{nodeId: nodes[0]})
		gossipers = append(gossipers, g)
	}

	// Let the nodes gossip
	time.Sleep(types.DEFAULT_GOSSIP_INTERVAL * time.Duration(len(nodes)))

	peers := make(map[types.NodeId]string)
	for i, ip := range nodes {
		nodeId := types.NodeId(strconv.FormatInt(int64(i), 10))
		peers[nodeId] = ip
	}
	// Update the cluster size to 5
	for i := 0; i < len(nodes); i++ {
		gossipers[i].UpdateCluster(peers)
	}

	time.Sleep(2 * time.Second)

	// Bring node 4 down.
	gossipers[4].Stop(types.DEFAULT_GOSSIP_INTERVAL * time.Duration(len(nodes)))
	//time.Sleep(types.DEFAULT_GOSSIP_INTERVAL * time.Duration(len(nodes)))

	time.Sleep(2 * time.Second)

	for i := 0; i < len(nodes)-1; i++ {
		if gossipers[i].GetSelfStatus() != types.NODE_STATUS_UP {
			t.Error("Expected Node ", i, " status to be ", types.NODE_STATUS_UP, " Got: ", gossipers[i].GetSelfStatus())
		}
	}

	// Bring node 3,node 2, node 1 down
	gossipers[3].Stop(types.DEFAULT_GOSSIP_INTERVAL * time.Duration(len(nodes)))
	gossipers[2].Stop(types.DEFAULT_GOSSIP_INTERVAL * time.Duration(len(nodes)))
	gossipers[1].Stop(types.DEFAULT_GOSSIP_INTERVAL * time.Duration(len(nodes)))

	time.Sleep(types.DEFAULT_GOSSIP_INTERVAL * time.Duration(len(nodes)+1))
	//time.Sleep(types.DEFAULT_GOSSIP_INTERVAL)

	if gossipers[0].GetSelfStatus() != types.NODE_STATUS_SUSPECT_NOT_IN_QUORUM {
		t.Error("Expected Node 0 status to be ", types.NODE_STATUS_SUSPECT_NOT_IN_QUORUM, " Got: ", gossipers[0].GetSelfStatus())
	}

	// Start Node 2
	gossipers[2].Start([]string{nodes[0]})
	gossipers[2].UpdateCluster(peers)

	time.Sleep(types.DEFAULT_GOSSIP_INTERVAL)

	// Node 0 still not in quorum. But should be up as quorum timeout not occured yet
	if gossipers[0].GetSelfStatus() != types.NODE_STATUS_SUSPECT_NOT_IN_QUORUM {
		t.Error("Expected Node 0  status to be ", types.NODE_STATUS_SUSPECT_NOT_IN_QUORUM, " Got: ", gossipers[0].GetSelfStatus())
	}

	// Sleep for quorum timeout to occur
	time.Sleep(gossipers[0].quorumTimeout + 2*time.Second)

	if gossipers[0].GetSelfStatus() != types.NODE_STATUS_NOT_IN_QUORUM {
		t.Error("Expected Node 0 status to be ", types.NODE_STATUS_NOT_IN_QUORUM, " Got: ", gossipers[0].GetSelfStatus())
	}

	// Start Node 1
	gossipers[1].Start([]string{nodes[0]})
	gossipers[1].UpdateCluster(peers)

	time.Sleep(time.Duration(2) * types.DEFAULT_GOSSIP_INTERVAL)

	// Node 0 should now be up
	if gossipers[0].GetSelfStatus() != types.NODE_STATUS_UP {
		t.Error("Expected Node 0 status to be ", types.NODE_STATUS_UP, " Got: ", gossipers[0].GetSelfStatus())
	}

}

func TestQuorumRemoveNodes(t *testing.T) {
	printTestInfo()

	nodes := []string{
		"127.0.0.1:9919",
		"127.0.0.2:9920",
		"127.0.0.3:9921",
		"127.0.0.4:9922",
	}

	peers := make(map[types.NodeId]string)
	for i, ip := range nodes {
		nodeId := types.NodeId(strconv.FormatInt(int64(i), 10))
		peers[nodeId] = ip
	}

	var gossipers []*GossiperImpl
	for i, ip := range nodes {
		nodeId := types.NodeId(strconv.FormatInt(int64(i), 10))
		var g *GossiperImpl
		if i == 0 {
			g, _ = startNode(t, ip, nodeId, []string{}, peers)
		} else {
			g, _ = startNode(t, ip, nodeId, []string{nodes[0]}, peers)
		}

		gossipers = append(gossipers, g)
	}

	// Lets sleep so that the nodes gossip and update their quorum
	time.Sleep(types.DEFAULT_GOSSIP_INTERVAL * time.Duration(len(nodes)+1))

	for i, g := range gossipers {
		if g.GetSelfStatus() != types.NODE_STATUS_UP {
			t.Error("Expected Node ", i, " status to be ", types.NODE_STATUS_UP, " Got: ", g.GetSelfStatus())
		}
	}

	// Bring node 3,node 2 down
	gossipers[3].Stop(types.DEFAULT_GOSSIP_INTERVAL * time.Duration(len(nodes)))
	gossipers[2].Stop(types.DEFAULT_GOSSIP_INTERVAL * time.Duration(len(nodes)))

	time.Sleep(types.DEFAULT_GOSSIP_INTERVAL * time.Duration(len(nodes)+1))

	for i := 0; i < 2; i++ {
		if gossipers[i].GetSelfStatus() != types.NODE_STATUS_SUSPECT_NOT_IN_QUORUM {
			t.Error("Expected Node ", i, " status to be ", types.NODE_STATUS_SUSPECT_NOT_IN_QUORUM, " Got: ", gossipers[i].GetSelfStatus())
		}
	}

	//Remove the two nodes
	delete(peers, types.NodeId("2"))
	delete(peers, types.NodeId("3"))
	gossipers[0].UpdateCluster(peers)
	gossipers[1].UpdateCluster(peers)

	time.Sleep(types.DEFAULT_GOSSIP_INTERVAL)

	for i := 0; i < 2; i++ {
		if gossipers[i].GetSelfStatus() != types.NODE_STATUS_UP {
			t.Error("Expected Node ", i, " status to be ", types.NODE_STATUS_UP, " Got: ", gossipers[i].GetSelfStatus())
		}
	}
}

func TestQuorumAddNodes(t *testing.T) {
	printTestInfo()
	node0Ip := "127.0.0.1:9923"
	node0 := types.NodeId("0")
	peers := make(map[types.NodeId]string)
	peers[node0] = node0Ip
	g0, _ := startNode(t, node0Ip, node0, []string{}, peers)

	// Lets sleep so that the nodes gossip and update their quorum
	time.Sleep(types.DEFAULT_GOSSIP_INTERVAL * time.Duration(1))

	if g0.GetSelfStatus() != types.NODE_STATUS_UP {
		t.Error("Expected Node 0 status to be ", types.NODE_STATUS_UP, " Got: ", g0.GetSelfStatus())
	}

	// Add a new node
	node1 := types.NodeId("1")
	node1Ip := "127.0.0.2:9924"
	peers[node1] = node1Ip
	g0.UpdateCluster(peers)

	time.Sleep(types.DEFAULT_GOSSIP_INTERVAL)
	if g0.GetSelfStatus() != types.NODE_STATUS_SUSPECT_NOT_IN_QUORUM {
		t.Error("Expected Node 0 status to be ", types.NODE_STATUS_SUSPECT_NOT_IN_QUORUM, " Got: ", g0.GetSelfStatus())
	}

	// Start the new node
	startNode(t, node1Ip, node1, []string{node0Ip}, peers)

	// Lets sleep so that the nodes gossip and update their quorum
	time.Sleep(types.DEFAULT_GOSSIP_INTERVAL * time.Duration(3))

	if g0.GetSelfStatus() != types.NODE_STATUS_UP {
		t.Error("Expected Node 0 status to be ", types.NODE_STATUS_UP, " Got: ", g0.GetSelfStatus())
	}
}
