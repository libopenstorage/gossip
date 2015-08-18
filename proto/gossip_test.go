package proto

import (
	"testing"
)

func TestAddRemoveNode(t *testing.T) {
	g := NewGossip("0.0.0.0:9010")

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
	err := g.AddNode("0.0.0.0:9010")
	if err == nil {
		t.Error("Duplicate node addition did not fail")
	}

	// check the nodelist is same
	peerNodes := g.GetNodes()
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

	g.Done()
}
