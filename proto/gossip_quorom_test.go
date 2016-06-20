package proto

import (
	"github.com/libopenstorage/gossip/types"
	"testing"
	"time"
	"strconv"
	"os/exec"
	//"strings"
)

func addKey(g *GossiperImpl) types.StoreKey {
	key := types.StoreKey("new_key")
	value := "new_value"
	g.UpdateSelf(key, value)
	return key
}

func addIptablesRule(portNumber, chain, protocol, rule string) error {
	ipTables := exec.Command("iptables", "-I", chain, "-p",protocol,"--dport", portNumber, "-j",rule)
	return ipTables.Run()	
}

func deleteIpTablesRule(portNumber, chain, protocol, rule string) error {
	ipTables := exec.Command("iptables", "-D", chain, "-p", protocol, "--dport", portNumber, "-j", rule)
	return ipTables.Run()
}

func bringPortDown(t *testing.T, portNumber string) error {
	err := addIptablesRule(portNumber, "INPUT", "tcp", "REJECT")
	if err != nil {
		t.Error("Unable to bring tcp port input down : ", err)
		return err
	}

	err = addIptablesRule(portNumber, "INPUT", "udp", "DROP")
	if err != nil {
		t.Error("Unable to bring udp port input down : ", err)
		dErr := deleteIpTablesRule(portNumber, "INPUT", "tcp", "REJECT")
		if dErr != nil {
			t.Error("Unable to delete ip tables rule. Check your iptables: iptables --list")
			return dErr
		}
		return err
	}

	err = addIptablesRule(portNumber, "OUTPUT", "tcp", "REJECT")
	if err != nil {
		t.Error("Unable to bring tcp port input down : ", err)
		if dErr := deleteIpTablesRule(portNumber, "INPUT", "tcp", "REJECT"); dErr != nil {
			t.Error("Unable to delete ip tables rule. Check your iptables: iptables --list")
			return dErr
		}
		if dErr := deleteIpTablesRule(portNumber, "INPUT", "udp", "DROP"); dErr != nil {
			t.Error("Unable to delete ip tables rule. Check your iptables: iptables --list")
			return dErr
		}
		return err
	}

	err = addIptablesRule(portNumber, "OUTPUT", "udp", "DROP")
	if err != nil {
		t.Error("Unable to bring udp port input down : ", err)
		if dErr := deleteIpTablesRule(portNumber, "INPUT", "tcp", "REJECT"); dErr != nil {
			t.Error("Unable to delete ip tables rule. Check your iptables: iptables --list")
			return dErr
		}
		if dErr := deleteIpTablesRule(portNumber, "INPUT", "udp", "DROP"); dErr != nil {
			t.Error("Unable to delete ip tables rule. Check your iptables: iptables --list")
			return dErr
		}
		if dErr := deleteIpTablesRule(portNumber, "OUTPUT", "tcp", "REJECT"); dErr != nil {
			t.Error("Unable to delete ip tables rule. Check your iptables: iptables --list")
			return dErr
		}
		return err
	}
	return nil
}

func bringPortUp(t *testing.T, portNumber string) error {
	if dErr := deleteIpTablesRule(portNumber, "INPUT", "tcp", "REJECT"); dErr != nil {
		t.Error("Unable to delete ip tables rule. Check your iptables: iptables --list")
		return dErr
	}
	if dErr := deleteIpTablesRule(portNumber, "INPUT", "udp", "DROP"); dErr != nil {
		t.Error("Unable to delete ip tables rule. Check your iptables: iptables --list")
		return dErr
		}
	if dErr := deleteIpTablesRule(portNumber, "OUTPUT", "tcp", "REJECT"); dErr != nil {
		t.Error("Unable to delete ip tables rule. Check your iptables: iptables --list")
		return dErr
	}
	if dErr := deleteIpTablesRule(portNumber, "OUTPUT", "udp", "DROP"); dErr != nil {
		t.Error("Unable to delete ip tables rule. Check your iptables: iptables --list")
		return dErr
	}
	
	return nil
}

func startNode(t *testing.T, selfIp string, nodeId types.NodeId, peerIps []string, clusterSize int) (*GossiperImpl, types.StoreKey) {
	g, _ := NewGossiperImpl(selfIp, nodeId, peerIps, types.DEFAULT_GOSSIP_VERSION)
	g.UpdateClusterSize(clusterSize)
	key := addKey(g)
	storeKv := g.GetStoreKeyValue(key)
	kv := storeKv[nodeId]
	if kv.Status != types.NODE_STATUS_WAITING_FOR_QUORUM {
		t.Error("Expected Node ", nodeId, " to have status: ", types.NODE_STATUS_WAITING_FOR_QUORUM, " Got: ", kv.Status)
	}

	return g, key
}


func TestAllNodesUpOneByOne(t *testing.T) {
	printTestInfo()

	nodes := []string{
		"127.0.0.1:9900",
		"127.0.0.2:9901",
	}

	// Start Node0 with cluster size 1
	node0 := types.NodeId("0")
	g0, _ := startNode(t, nodes[0], node0, []string{}, 1)
	
	// Handle Quorum manually
	g0.CheckAndUpdateQuorum()
	if g0.GetSelfStatus() != types.NODE_STATUS_UP {
		t.Error("Expected Node 0 to have status: ", types.NODE_STATUS_UP)
	}

	// Start Node1 with cluster size 2
	node1 := types.NodeId("1")
	g1, _ := startNode(t, nodes[1], node1, []string{nodes[0]}, 2)	

	// Handle Quorum manually
	g1.CheckAndUpdateQuorum()
	if g1.GetSelfStatus() != types.NODE_STATUS_UP {
		t.Error("Expected Node 1 to have status: ", types.NODE_STATUS_UP)
	}

	// Check if Node0 is still Up
	if g0.GetSelfStatus() != types.NODE_STATUS_UP {
		t.Error("Expected Node 0 to have status: ", types.NODE_STATUS_UP)
	}
}

func TestNodeLoosesQuorumAndGainsBack(t *testing.T) {
	printTestInfo()

	nodes := []string{
		"127.0.0.1:9902",
		"127.0.0.2:9903",
	}
	
	node0 := types.NodeId("0")
	
	// Start Node 0
	g0, _ := startNode(t, nodes[0], node0, []string{}, 1)
	
	// Handle Quorum manually
	g0.CheckAndUpdateQuorum()
	if g0.GetSelfStatus() != types.NODE_STATUS_UP {
		t.Error("Expected Node 0 to have status: ", types.NODE_STATUS_UP)
	}

	// Simulate new node was added by updating the cluster size, but the new node is not talking to node0
	// Node 0 should loose quorom 1/2
	g0.UpdateClusterSize(2)
	// Update the quorum manually. In real scenario, this will be done automatically by the go thread
	g0.CheckAndUpdateQuorum()
	if g0.GetSelfStatus() != types.NODE_STATUS_UP_AND_WAITING_FOR_QUORUM {
		t.Error("Expected Node 0 to have status: ", types.NODE_STATUS_UP_AND_WAITING_FOR_QUORUM)
	}

	// Lets start the actual Node 1
	node1 := types.NodeId("1")
	g1, _ := startNode(t, nodes[1], node1, []string{nodes[0]}, 2)

	// Sleep so that nodes gossip
	time.Sleep(g1.GossipInterval() * time.Duration(len(nodes) + 1))

	// Update the quorum for both the nodes manually	
	g1.CheckAndUpdateQuorum()
	g0.CheckAndUpdateQuorum()
	if g0.GetSelfStatus() != types.NODE_STATUS_UP {
		t.Error("Expected Node 0 to have status: ", types.NODE_STATUS_UP)
	}
	if g1.GetSelfStatus() != types.NODE_STATUS_UP {
		t.Error("Expected Node 1 to have status: ", types.NODE_STATUS_UP)
	}	
}

func TestTwoNodesLooseConnectivity(t *testing.T) {
	printTestInfo()

	nodes := []string{
		"127.0.0.1:9904",
		"127.0.0.2:9905",
	}

	node0 := types.NodeId("0")
	g0, _ := startNode(t, nodes[0], node0, []string{}, 1)
	g0.CheckAndUpdateQuorum()	
	if g0.GetSelfStatus() != types.NODE_STATUS_UP {
		t.Error("Expected Node 0 to have status: ", types.NODE_STATUS_UP)
	}

	// Simulate new node was added by updating the cluster size, but the new node is not talking to node0
	// Node 0 should loose quorom 1/2
	g0.UpdateClusterSize(2)
	// Update the quorum manually. In real scenario, this will be done automatically by the go thread
	g0.CheckAndUpdateQuorum()
	if g0.GetSelfStatus() != types.NODE_STATUS_UP_AND_WAITING_FOR_QUORUM {
		t.Error("Expected Node 0 to have status: ", types.NODE_STATUS_UP_AND_WAITING_FOR_QUORUM)
	}

	// Lets start the actual node 1. We do not supply node 0 Ip address here so that node 1 does not talk to node 0
	// to simulate NO connectivity between node 0 and node 1
	node1 := types.NodeId("1")
	g1, _ := startNode(t, nodes[1], node1, []string{}, 2)

	// For node 0 the status will change from UP_WAITING_QUORUM to WAITING_QUORUM after
	// the quorum timeout
	time.Sleep(TestQuorumTimeout)

	// Update the quorum for both the nodes manually	
	g1.CheckAndUpdateQuorum()
	g0.CheckAndUpdateQuorum()
	if g0.GetSelfStatus() != types.NODE_STATUS_WAITING_FOR_QUORUM {
		t.Error("Expected Node 0 to have status: ", types.NODE_STATUS_WAITING_FOR_QUORUM)
	}
	if g1.GetSelfStatus() != types.NODE_STATUS_WAITING_FOR_QUORUM {
		t.Error("Expected Node 1 to have status: ", types.NODE_STATUS_WAITING_FOR_QUORUM)
	}	
}

func TestOneNodeIsolated(t *testing.T) {
	printTestInfo()
	
	nodes := []string{
		"127.0.0.1:9906",
		"127.0.0.2:9907",
		"127.0.0.3:9908",
	}

	var gossipers []*GossiperImpl
	for i, ip := range nodes {
		nodeId := types.NodeId(strconv.FormatInt(int64(i), 10))
		var g *GossiperImpl
		if i == 0 {
			g, _ = startNode(t, ip, nodeId, []string{}, len(nodes))
		} else {
			g, _ = startNode(t, ip, nodeId, []string{nodes[0]}, len(nodes))
		}
		
		// As we have set the cluster size to 3, none of the nodes should be Up at start
		if g.GetSelfStatus() != types.NODE_STATUS_WAITING_FOR_QUORUM {
			t.Error("Expected Node ", i, " status to be ", types.NODE_STATUS_WAITING_FOR_QUORUM)
		}
		
		gossipers = append(gossipers, g)
	}

	// Lets sleep so that the nodes gossip and update their quorum
	time.Sleep(types.DEFAULT_GOSSIP_INTERVAL * time.Duration(len(nodes) + 1))

	for i, g := range gossipers {
		if g.GetSelfStatus() != types.NODE_STATUS_UP {
			t.Error("Expected Node ", i, " status to be ", types.NODE_STATUS_UP, " Got: ", g.GetSelfStatus())
		}
	}
	
	// Isolate node 1
	// Simulate isolation by stopping gossiper for node 1 and starting it back,
	// but by not providing peer IPs and setting cluster size to 3.
	gossipers[1].Stop(time.Duration(10)*time.Second)
	gossipers[1].InitStore(types.NodeId("1"), "v1")
	gossipers[1].Start([]string{})

	// Lets sleep so that the nodes gossip and update their quorum
	time.Sleep(types.DEFAULT_GOSSIP_INTERVAL * time.Duration(len(nodes) + 1))

	for i, g := range gossipers {
		if i == 1 {
			if g.GetSelfStatus() != types.NODE_STATUS_WAITING_FOR_QUORUM {
				t.Error("Expected Node ", i, " status to be ", types.NODE_STATUS_WAITING_FOR_QUORUM, " Got: ", g.GetSelfStatus())
			}
			continue
		}
		if g.GetSelfStatus() != types.NODE_STATUS_UP {
			t.Error("Expected Node ", i, " status to be ", types.NODE_STATUS_UP, " Got: ", g.GetSelfStatus())
		}
	}
}

func TestNetworkPartition(t *testing.T) {
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
	for i:=0;i<3;i++ {
		nodeId := types.NodeId(strconv.FormatInt(int64(i), 10))
		var g *GossiperImpl
		g, _ = startNode(t, nodes[i], nodeId, []string{nodes[0], nodes[1], nodes[2]}, 3)
		gossipers = append(gossipers, g)
	}
	// Parition 2
	for i:=3;i<5;i++ {
		nodeId := types.NodeId(strconv.FormatInt(int64(i), 10))
		var g *GossiperImpl
		g, _ = startNode(t, nodes[i], nodeId, []string{nodes[3], nodes[4]}, 2)
		gossipers = append(gossipers, g)		
	}
	// Let the nodes gossip
	time.Sleep(types.DEFAULT_GOSSIP_INTERVAL * time.Duration(len(nodes)))
	for i, g := range gossipers {
		if g.GetSelfStatus() != types.NODE_STATUS_UP {
			t.Error("Expected Node ", i, " status to be ", types.NODE_STATUS_UP, " Got: ", g.GetSelfStatus())
		}
	}

	// Setup the partition by updating the cluster size
	for _,g := range gossipers {
		g.UpdateClusterSize(5)
	}

	// Let the nodes update their quorum
	time.Sleep(time.Duration(3) * time.Second)
	// Partition 1
	for i:=0;i<3;i++ {
		if gossipers[i].GetSelfStatus() != types.NODE_STATUS_UP {
			t.Error("Expected Node ", i, " status to be ", types.NODE_STATUS_UP, " Got: ", gossipers[i].GetSelfStatus())
		}

	}
	// Parition 2
	for i:=3;i<5;i++ {
		if gossipers[i].GetSelfStatus() != types.NODE_STATUS_UP_AND_WAITING_FOR_QUORUM {
			t.Error("Expected Node ", i, " status to be ", types.NODE_STATUS_UP_AND_WAITING_FOR_QUORUM, " Got: ", gossipers[i].GetSelfStatus())
		}
	}

	time.Sleep(TestQuorumTimeout)
	// Parition 2
	for i:=3;i<5;i++ {
		if gossipers[i].GetSelfStatus() != types.NODE_STATUS_WAITING_FOR_QUORUM {
			t.Error("Expected Node ", i, " status to be ", types.NODE_STATUS_WAITING_FOR_QUORUM, " Got: ", gossipers[i].GetSelfStatus())
		}
	}
}


func TestQuorum(t *testing.T) {
	TestAllNodesUpOneByOne(t)	
	TestTwoNodesLooseConnectivity(t)
	TestOneNodeIsolated(t)
	TestNetworkPartition(t)
}
