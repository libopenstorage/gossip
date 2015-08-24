package proto

import (
	"github.com/libopenstorage/gossip/api"
	"testing"
	"time"
)

func TestGossiperAddRemoveGetNode(t *testing.T) {
	printTestInfo()
	g := NewGossiper("0.0.0.0:9010", 1)

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
	g := NewGossiper("0.0.0.0:9092", 1)

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

	// stay up for more than gossip interval and check
	// that we don't die because there is no one to gossip
	time.Sleep(2 * gossipDuration)

	g.Stop()
}

func TestGossiperSimpleGossip(t *testing.T) {
	printTestInfo()
	g1 := NewGossiper("0.0.0.0:9052", 1)
	g2 := NewGossiper("0.0.0.0:9072", 2)

	g1.SetGossipInterval(1500 * time.Millisecond)
	g2.SetGossipInterval(2400 * time.Millisecond)

	err := g1.AddNode("0.0.0.0:9072")
	if err != nil {
		t.Error("Unexpected error adding node to g1: ", err)
	}

	err = g2.AddNode("0.0.0.0:9052")
	if err != nil {
		t.Error("Unexpected error adding node to g2: ", err)
	}

	// let the nodes gossip about nothing ;)
	time.Sleep(2 * time.Second)

	g1.UpdateSelf("g1key", "somevalue")
	g2.UpdateSelf("g2key", "g2value")

	time.Sleep(10 * time.Second)

	g1.Stop()
	g2.Stop()

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

		if len(g1Values.List) != len(g2Values.List) {
			t.Fatal("Lens mismatch between g1 and g2 values")
		}

		i := api.NodeId(0)
		for ; i < api.NodeId(len(g1Values.List)); i++ {
			if g1Values.List[i].Id != g2Values.List[i].Id {
				t.Error("Values mismtach between g1 and g2, g1:\n",
					g1Values.List[i].Id, "\ng2:", g2Values.List[i].Id)
			}
		}
	}
}
