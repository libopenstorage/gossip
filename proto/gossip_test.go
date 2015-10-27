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
	g.Init(ip, selfNodeId)
	g.Start()
	return g
}

func TestGossiperAddRemoveGetNode(t *testing.T) {
	printTestInfo()
	g := NewGossiperImpl("0.0.0.0:9010", "1")

	nodes := []string{"0.0.0.0:90011",
		"0.0.0.0:90012", "0.0.0.0:90013",
		"0.0.0.0:90014"}

	// test add nodes
	for _, node := range nodes {
		err := g.AddNode(node)
		if err != nil {
			t.Error("Error adding new node")
		}
	}

	// try adding existing node
	err := g.AddNode(nodes[0])
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

func TestGossiperMisc(t *testing.T) {
	printTestInfo()
	g := NewGossiperImpl("0.0.0.0:9092", "1")

	// get the default value
	gossipIntvl := g.GossipInterval()
	if gossipIntvl == 0 {
		t.Error("Default gossip interval set to zero")
	}

	gossipDuration := 1 * time.Second
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

	deathDuration := 1 * time.Second
	g.SetNodeDeathInterval(deathDuration)
	deathIntvl = g.NodeDeathInterval()
	if deathIntvl != deathDuration {
		t.Error("Set death interval and get interval differ, got: ", deathIntvl)
	}

	// stay up for more than gossip interval and check
	// that we don't die because there is no one to gossip
	time.Sleep(2 * gossipDuration)

	g.Stop()
}

func verifyGossiperEquality(g1 *GossiperImpl, g2 *GossiperImpl, t *testing.T) {
	// check for the equality
	g1Keys := g1.GetStoreKeys()
	g2Keys := g2.GetStoreKeys()
	if len(g1Keys) != len(g2Keys) {
		t.Error("Keys mismatch, g1: ", g1Keys, " g2:", g2Keys)
	}

	for _, key := range g1Keys {
		g1Values := g1.GetStoreKeyValue(key)
		g2Values := g1.GetStoreKeyValue(key)

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
		g := NewGossiperImpl(nodeId, types.NodeId(i))

		g.SetGossipInterval(time.Duration(1500+rand.Intn(200)) * time.Millisecond)
		// add one neighbor and 2 random peers
		if i < len(nodes)-1 {
			err := g.AddNode(nodes[i+1])
			if err != nil {
				t.Error("Unexpected error adding node to id: ", nodeId,
					" node: ", nodes[i+1])
			}
		} else {
			err := g.AddNode(nodes[0])
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

			err := g.AddNode(nodes[randId])
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
			t.Log("Updting data for ", id)
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
