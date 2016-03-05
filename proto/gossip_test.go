package proto

import (
	"github.com/libopenstorage/gossip/types"
	"math/rand"
	"strconv"
	"testing"
	"time"
)

// New returns an initialized Gossip node
// which identifies itself with the given ip
func NewGossiperImpl(ip string, selfNodeId types.NodeId) *GossiperImpl {
	g := new(GossiperImpl)
	g.Init(ip, selfNodeId, 1)
	g.selfCorrect = false
	g.Start()
	return g
}

func TestGossiperHistory(t *testing.T) {
	var maxLen uint8 = 5
	h := NewGossipHistory(maxLen)

	for i := 0; i < 2*int(maxLen); i++ {
		h.AddLatest(NewGossipSessionInfo(strconv.Itoa(i),
			types.GD_ME_TO_PEER))
		if i < 5 {
			records := h.GetAllRecords()
			if len(records) != i+1 {
				t.Error("Length of returned records don't match, r:", len(records),
					" expected: ", h.nodes.Len())
			}
		}
	}

	if h.nodes.Len() != int(maxLen) {
		t.Error("Len mismatch h: ", h.nodes.Len(), " expected: ", maxLen)
	}

	records := h.GetAllRecords()
	if len(records) != h.nodes.Len() {
		t.Error("Length of returned records don't match, r:", len(records),
			" expected: ", h.nodes.Len())
	}

	var p *types.GossipSessionInfo = nil
	for _, c := range records {
		if p != nil {
			pId, ok3 := strconv.Atoi(p.Node)
			cId, ok4 := strconv.Atoi(c.Node)

			if ok3 != nil || ok4 != nil {
				t.Error("Failed to get elements: p: ", p, " c: ", c)
				continue
			}

			if pId < cId {
				t.Error("Data maintained in wrong order ", p, " c: ", c)
			}

			if p.Ts.Before(c.Ts) {
				t.Error("Data maintained in wrong order ", p, " c: ", c)
			}
		}
		p = c
	}

}

func TestGossiperAddRemoveGetNode(t *testing.T) {
	printTestInfo()
	g := NewGossiperImpl("0.0.0.0:9010", "0")

	nodes := []string{"0.0.0.0:9011",
		"0.0.0.0:9012", "0.0.0.0:9013",
		"0.0.0.0:9014"}

	// test add nodes
	i := 1
	for _, node := range nodes {
		err := g.AddNode(node, types.NodeId(strconv.Itoa(i)))
		if err != nil {
			t.Error("Error adding new node")
		}
		i++
	}

	// try adding existing node
	err := g.AddNode(nodes[0], types.NodeId(strconv.Itoa(1)))
	if err == nil {
		t.Error("Duplicate node addition did not fail")
	}

	// check the nodelist is same
	peerNodes := g.GetNodes()
	if len(peerNodes) != len(nodes) {
		t.Error("Peer nodes len does not match added nodes, got: ",
			peerNodes, " expected: ", nodes)
	}
outer:
	for _, origNode := range nodes {
		for _, peerNode := range peerNodes {
			if origNode == peerNode {
				continue outer
			}
		}
		t.Error("Peer nodes does not have added node: ", origNode)
	}

	// test remove nodes
	for _, node := range nodes {
		err := g.RemoveNode(node)
		if err != nil {
			t.Error("Error removing new node")
		}
	}

	// try removing non-existing node
	err = g.RemoveNode("0.0.0.0:9020")
	if err == nil {
		t.Error("Non-existing node removal did not fail")
	}

	g.Stop()
}

func TestGossiperOnlyOneNodeGossips(t *testing.T) {
	printTestInfo()

	nodes := []string{"0.0.0.0:9222", "0.0.0.0:9223",
		"0.0.0.0:9224"}

	rand.Seed(time.Now().UnixNano())
	id := types.NodeId(strconv.Itoa(0))
	gZero := NewGossiperImpl(nodes[0], id)
	gZero.SetGossipInterval(200 * time.Millisecond)
	gZero.SetNodeDeathInterval(200 * time.Millisecond)
	for j, peer := range nodes {
		if j == 0 {
			continue
		}
		gZero.AddNode(peer, types.NodeId(strconv.Itoa(j)))
	}

	// each node must mark node 0 as down
	key := types.StoreKey("somekey")
	value := "someValue"
	gZero.UpdateSelf(key, value)

	time.Sleep(5 * time.Second)

	res := gZero.GetStoreKeyValue(key)
	if len(res) != 3 {
		t.Error("Nodes down not reported ", res)
	}

	for nodeId, n := range res {
		if nodeId != n.Id {
			t.Error("Gossiper Id does not match ",
				nodeId, " n:", n.Id)
		}
		nid, ok := strconv.Atoi(string(nodeId))
		if ok != nil {
			t.Error("Failed to convert node to id ", nodeId, " n.Id", n.Id)
		}
		t.Log("Node id is ", nid)
		if nid != 0 {
			if n.Status != types.NODE_STATUS_DOWN {
				t.Error("Gossiper ", nid,
					"Expected node status not to be down: ", nodeId, " n:", n)
			}
		}
	}

	gZero.Stop()
}

func TestGossiperOneNodeNeverGossips(t *testing.T) {
	printTestInfo()

	nodes := []string{"0.0.0.0:9622", "0.0.0.0:9623",
		"0.0.0.0:9624"}

	rand.Seed(time.Now().UnixNano())
	gossipers := make(map[int]*GossiperImpl)
	for i, nodeId := range nodes {
		if i == 0 {
			// node 0 never comes up
			continue
		}
		id := types.NodeId(strconv.Itoa(i))
		g := NewGossiperImpl(nodeId, id)
		g.SetGossipInterval(time.Duration(200+rand.Intn(200)) * time.Millisecond)
		for j, peer := range nodes {
			if i == j {
				continue
			}
			g.AddNode(peer, types.NodeId(j))
		}
		gossipers[i] = g
	}

	// each node must mark node 0 as down
	key := types.StoreKey("somekey")
	value := "someValue"
	for i, g := range gossipers {
		g.UpdateSelf(key, value+strconv.Itoa(i))
	}

	for i, g := range gossipers {
		res := g.GetStoreKeyValue(key)
		for nodeId, n := range res {
			if nodeId != n.Id {
				t.Error("Gossiper ", i, "Id does not match ",
					nodeId, " n:", n.Id)
			}
			nid, ok := strconv.Atoi(string(nodeId))
			if ok != nil {
				t.Error("Failed to convert node to id ", nodeId, " n.Id", n.Id)
			}
			if nid == 0 {
				if n.Status == types.NODE_STATUS_DOWN {
					t.Error("Gossiper ", i,
						"Expected node status not to be down: ", nodeId, " n:", n)
				}
			}
		}
	}

	time.Sleep(2 * time.Second)
	for i, g := range gossipers {
		res := g.GetStoreKeyValue(key)
		for nodeId, n := range res {
			if nodeId != n.Id {
				t.Error("Gossiper ", i, "Id does not match ",
					nodeId, " n:", n.Id)
			}
			nid, ok := strconv.Atoi(string(nodeId))
			if ok != nil {
				t.Error("Failed to convert node to id ", nodeId, " n.Id", n.Id)
			}
			if nid == 0 {
				if n.Status != types.NODE_STATUS_DOWN {
					t.Error("Gossiper ", i,
						"Expected node status to be down: ", nodeId, " n:", n)
				}
			} else {
				if n.Status != types.NODE_STATUS_UP {
					t.Error("Gossiper ", i, "Expected node to be up: ", nodeId,
						" n:", n)
				}
			}
		}
	}

	for _, g := range gossipers {
		g.Stop()
	}
}

func TestGossiperUpdateNodeIp(t *testing.T) {
	printTestInfo()

	nodes := []string{"0.0.0.0:9325", "0.0.0.0:9326", "0.0.0.0:9327"}

	rand.Seed(time.Now().UnixNano())
	gossipers := make(map[int]*GossiperImpl)
	for i, nodeId := range nodes {
		id := types.NodeId(strconv.Itoa(i))
		g := NewGossiperImpl(nodeId, id)

		g.SetGossipInterval(time.Duration(200+rand.Intn(200)) * time.Millisecond)
		for j, peer := range nodes {
			if i == j {
				continue
			}
			peerIp := peer
			if j == 0 {
				peerIp = "0.0.0.0:11000"
			}
			g.AddNode(peerIp, types.NodeId(j))
		}
		gossipers[i] = g
	}

	// each node must mark node 0 as down
	key := types.StoreKey("somekey")
	value := "someValue"
	for i, g := range gossipers {
		g.UpdateSelf(key, value+strconv.Itoa(i))
	}

	for k := 0; k < 3; k++ {
		time.Sleep(2 * time.Second)
		if k == 1 {
			for i, g := range gossipers {
				if i == 0 {
					continue
				}
				err := g.UpdateNode(nodes[0], types.NodeId(0))
				if err != nil {
					t.Error("Error updating node ", i, " : ", err)
				}
			}

			for i, g := range gossipers {
				peerNodes := g.GetNodes()
				if len(peerNodes) != len(nodes)-1 {
					t.Error("Peer nodes len does not match added nodes, got: ",
						peerNodes, " expected: ", nodes)
				}
				for _, node := range peerNodes {
					found := false
					for _, origNode := range nodes {
						if origNode == node {
							found = true
							break
						}
					}
					if !found {
						t.Error("Could not find node ", node,
							", for gossiper ", i)
					}
				}
			}
		}
		for i, g := range gossipers {
			res := g.GetStoreKeyValue(key)
			for nodeId, n := range res {
				if nodeId != n.Id {
					t.Error("Gossiper ", i, "Id does not match ",
						nodeId, " n:", n.Id)
				}
				_, ok := strconv.Atoi(string(nodeId))
				if ok != nil {
					t.Error("Failed to convert node to id ", nodeId, " n.Id", n.Id)
				}
				// All nodes must be up
				if n.Status != types.NODE_STATUS_UP {
					t.Error("Gossiper ", i,
						"Expected node status to be up: ", nodeId, " n:", n)
				}
			}
		}
	}
}

func TestGossiperGossipMarkOldGenNode(t *testing.T) {
	printTestInfo()

	nodes := []string{"0.0.0.0:9225", "0.0.0.0:9226", "0.0.0.0:9227"}

	rand.Seed(time.Now().UnixNano())
	gossipers := make(map[int]*GossiperImpl)
	for i, nodeId := range nodes {
		if i == 0 {
			// node 0 never comes up
			continue
		}
		id := types.NodeId(strconv.Itoa(i))
		g := NewGossiperImpl(nodeId, id)

		g.SetGossipInterval(time.Duration(200+rand.Intn(200)) * time.Millisecond)
		g.SetNodeDeathInterval(200 * time.Millisecond)
		for j, peer := range nodes {
			if i == j {
				continue
			}
			g.AddNode(peer, types.NodeId(strconv.Itoa(j)))
		}
		gossipers[i] = g
	}

	// each node must mark node 0 as down
	key := types.StoreKey("somekey")
	value := "someValue"
	for i, g := range gossipers {
		g.UpdateSelf(key, value+strconv.Itoa(i))
	}

	for i, g := range gossipers {
		res := g.GetStoreKeyValue(key)
		for nodeId, n := range res {
			if nodeId != n.Id {
				t.Error("Gossiper ", i, "Id does not match ",
					nodeId, " n:", n.Id)
			}
			nid, ok := strconv.Atoi(string(nodeId))
			if ok != nil {
				t.Error("Failed to convert node to id ", nodeId, " n.Id", n.Id)
			}
			if nid == 0 {
				if n.Status == types.NODE_STATUS_DOWN {
					t.Error("Gossiper ", i,
						"Expected node status not to be down: ", nodeId, " n:", n)
				}
			}
		}
	}

	// Now Reset both node 0 and node 1 in node 2.
	nid0 := types.NodeId(strconv.Itoa(0))
	nid1 := types.NodeId(strconv.Itoa(1))
	nid2 := types.NodeId(strconv.Itoa(2))

	g, _ := gossipers[2]
	g.MarkNodeHasOldGen(nid0)
	g.MarkNodeHasOldGen(nid1)
	// Update value in node 1
	g1, _ := gossipers[1]
	g1.UpdateSelf(key, value+"__1")

	// Node must be up now
	g, _ = gossipers[2]
	res := g.GetStoreKeyValue(key)
	for nodeId, n := range res {
		if nid2 == nodeId {
			continue
		}
		if nodeId != n.Id {
			t.Error("Id does not match ", nodeId, " n:", n.Id)
		}
		_, ok := strconv.Atoi(string(nodeId))
		if ok != nil {
			t.Error("Failed to convert node to id ", nodeId, " n.Id", n.Id)
		}
		if n.Status != types.NODE_STATUS_WAITING_FOR_NEW_UPDATE {
			t.Error("Expected node status to be down: ", nodeId, " n:", n)
		}
	}

	time.Sleep(5 * time.Second)
	res = g.GetStoreKeyValue(key)
	g0Node, _ := res[nid0]
	if g0Node.Status != types.NODE_STATUS_DOWN_WAITING_FOR_NEW_UPDATE {
		t.Error("Expected node to be down waiting for update ", g0Node.Status,
			" down:", types.NODE_STATUS_DOWN)
	}

	// Test that the node does not is marked down after reset
	g1, _ = gossipers[1]
	g1.Stop()
	g.MarkNodeHasOldGen(nid1)
	time.Sleep(5 * time.Second)
	res = g.GetStoreKeyValue(key)
	g1Node, _ := res[nid1]
	if g1Node.Status != types.NODE_STATUS_DOWN_WAITING_FOR_NEW_UPDATE {
		t.Error("Expected node to be down waiting for update ", g1Node)
	}

	// Now check transition from node status down to node up
	// Increase death interval and update g1
	g1.Start()
	g.SetNodeDeathInterval(30 * time.Second)
	// Allow some gossip to occur
	time.Sleep(1 * time.Second)
	res = g.GetStoreKeyValue(key)
	g1Node, _ = res[nid1]
	if g1Node.Status != types.NODE_STATUS_UP {
		t.Error("Expected node to be up ", g1Node.Status,
			" up: ", types.NODE_STATUS_UP)
	}

	for _, g := range gossipers {
		g.Stop()
	}
}

func TestGossiperZMisc(t *testing.T) {
	printTestInfo()
	g := NewGossiperImpl("0.0.0.0:9092", "1")
	g.selfCorrect = true

	key := types.StoreKey("someKey")
	g.UpdateSelf(key, "1")
	startTime := time.Now()

	// get the default value
	gossipIntvl := g.GossipInterval()
	if gossipIntvl == 0 {
		t.Error("Default gossip interval set to zero")
	}

	gossipDuration := 20 * time.Millisecond
	g.SetGossipInterval(gossipDuration)
	gossipIntvl = g.GossipInterval()
	if gossipIntvl != gossipDuration {
		t.Error("Set interval and get interval differ, got: ", gossipIntvl)
	}

	// get the default value
	deathIntvl := g.NodeDeathInterval()
	if deathIntvl == 0 {
		t.Error("Default death interval set to zero")
	}

	deathDuration := 20 * time.Millisecond
	g.SetNodeDeathInterval(deathDuration)
	deathIntvl = g.NodeDeathInterval()
	if deathIntvl != deathDuration {
		t.Error("Set death interval and get interval differ, got: ", deathIntvl)
	}

	// stay up for more than gossip interval and check
	// that we don't die because there is no one to gossip
	time.Sleep(18 * gossipDuration)

	// check that our value is self corrected
	gValues := g.GetStoreKeyValue(key)
	if len(gValues) > 1 {
		t.Error("More values returned than request ", gValues)
	}
	nodeValue, ok := gValues["1"]
	if !ok {
		t.Error("Could not get node value for self node")
	} else {
		if !nodeValue.LastUpdateTs.After(startTime) {
			t.Error("time not updated")
		}
	}

	g.Stop()
}

func verifyGossiperEquality(g1 *GossiperImpl, g2 *GossiperImpl, t *testing.T) {
	// check for the equality
	g1Keys := g1.GetStoreKeys()

	for _, key := range g1Keys {
		g1Values := g1.GetStoreKeyValue(key)
		g2Values := g1.GetStoreKeyValue(key)

		t.Log("Key: ", key)
		t.Log("g1Values: ", g1Values)
		t.Log("g2Values: ", g2Values)

		if len(g1Values) != len(g2Values) {
			t.Fatal("Lens mismatch between g1 and g2 values")
		}

		for i := 0; i < len(g1Values); i++ {
			id := types.NodeId(strconv.Itoa(i))
			if g1Values[id].Id != g2Values[id].Id {
				t.Error("Values mismtach between g1 and g2, g1:\n",
					g1Values[id].Id, "\ng2:", g2Values[id].Id)
			}
		}
	}
}

func TestGossiperMultipleNodesGoingUpDown(t *testing.T) {
	printTestInfo()

	nodes := []string{"0.0.0.0:9152", "0.0.0.0:9153",
		"0.0.0.0:9154", "0.0.0.0:9155",
		"0.0.0.0:9156", "0.0.0.0:9157",
		"0.0.0.0:9158", "0.0.0.0:9159",
		"0.0.0.0:9160", "0.0.0.0:9161"}

	rand.Seed(time.Now().UnixNano())
	gossipers := make(map[string]*GossiperImpl)
	for i, nodeId := range nodes {
		g := NewGossiperImpl(nodeId, types.NodeId(strconv.Itoa(i)))

		g.SetGossipInterval(time.Duration(1500+rand.Intn(200)) * time.Millisecond)
		// add one neighbor and 2 random peers
		if i < len(nodes)-2 {
			err := g.AddNode(nodes[i+1], types.NodeId(strconv.Itoa(i)))
			if err != nil {
				t.Error("Unexpected error adding node to id: ", nodeId,
					" node: ", nodes[i+1])
			}
		} else {
			err := g.AddNode(nodes[0], types.NodeId(strconv.Itoa(0)))
			if err != nil {
				t.Error("Unexpected error adding node to id: ", nodeId,
					" node: ", nodes[0])
			}
		}

		// to this gossiper, add two random peers
		for count := 0; count < 2; {
			randId := rand.Intn(len(nodes))
			if randId == i {
				continue
			}

			err := g.AddNode(nodes[randId], types.NodeId(strconv.Itoa(randId)))
			if err != nil {
				t.Log("Unexpected error adding node to id: ", nodeId,
					" node: ", nodes[randId], " err: ", err)
			} else {
				count++
			}
		}
		gossipers[nodeId] = g
		time.Sleep(2000 * time.Millisecond)
	}

	updateFunc := func(g *GossiperImpl, id string, max int, t *testing.T) {
		for i := 0; i < max; i++ {
			t.Log("Updting data for ", g.NodeId())
			g.UpdateSelf("sameKey", strconv.Itoa(i))
			g.UpdateSelf(types.StoreKey(g.NodeId()), strconv.Itoa(i*i))
			time.Sleep(g.GossipInterval() + time.Duration(rand.Intn(100)))
		}
	}

	for id, g := range gossipers {
		go updateFunc(g, id, 10, t)
	}

	// Max duration for update is 1500 + 200 + 100 per update * 10
	// = 1800 mil * 10 = 18000 mil.
	// To add go fork thread, 2000 mil on top.
	// Let gossip go on for another 10 seconds, after which it must settle
	time.Sleep(1 * time.Minute)

	// verify all of them are same
	for i := 1; i < len(nodes); i++ {
		t.Log("Checking equality of ", nodes[0], " and ", nodes[i])
		verifyGossiperEquality(gossipers[nodes[0]], gossipers[nodes[i]], t)
	}

	// start another update round, however, we will shut down soem machines
	// in between
	for id, g := range gossipers {
		go updateFunc(g, id, 10, t)
	}

	shutdownNodes := make(map[int]bool)
	for {
		randId := rand.Intn(len(nodes))
		if randId == 0 {
			continue
		}
		_, ok := shutdownNodes[randId]
		if ok == false {
			shutdownNodes[randId] = true
			gossipers[nodes[randId]].Stop()
			t.Log("Shutdown node ", nodes[randId])
			if len(shutdownNodes) == 3 {
				break
			}
		}
	}

	time.Sleep(1 * time.Minute)
	// verify all of them are same
	for i := 1; i < len(nodes); i++ {
		_, ok := shutdownNodes[i]
		if ok {
			continue
		}
		t.Log("Checking equality of ", nodes[0], " and ", nodes[i])
		verifyGossiperEquality(gossipers[nodes[0]], gossipers[nodes[i]], t)

		g := gossipers[nodes[i]]
		keys := g.GetStoreKeys()
		for _, key := range keys {
			values := g.GetStoreKeyValue(key)

			for j, nodeInfo := range values {
				nodeId, _ := strconv.Atoi(string(j))
				_, ok := shutdownNodes[nodeId]
				if ok && nodeInfo.Status == types.NODE_STATUS_UP {
					t.Error("Node not marked down: ", nodeInfo, " for node: ", nodes[i])
				}
			}
		}
	}

	for i := 1; i < len(nodes); i++ {
		gossipers[nodes[i]].Stop()
	}

}
