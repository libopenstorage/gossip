package proto

import (
	"fmt"
	"reflect"
	"testing"
	"time"
)

const (
	CPU    string = "CPU"
	Memory string = "Memory"
)

func printTest(name string) {
	fmt.Println("************* ", name, " *************")
}

func fillUpNode(node *NodeInfo, i int) {
	node.Id = NodeId(i + 1)
	node.LastUpdateTs = time.Now()

	value := make(map[string]NodeId)
	value[CPU] = node.Id
	value[Memory] = node.Id
	node.Value = value
}

func fillUpNodeInfo(nodes *NodeValue) {
	for i := 0; i < len(nodes.Nodes); i++ {
		fillUpNode(&nodes.Nodes[i], i)
	}
}

func verifyMetaInfo(nodes *NodeValue,
	t *testing.T) {
	m := nodes.MetaInfo()
	metaInfo, ok := m.(NodeMetaInfoList)
	if !ok {
		t.Error("Invalid type returned for metaInfo ",
			reflect.TypeOf(metaInfo))
	}

	// check len
	if len(metaInfo.MetaInfos) != len(nodes.Nodes) {
		t.Error("MetaInfo len ", len(metaInfo.MetaInfos),
			" does not match nodes len ", len(nodes.Nodes))
	}

	// check empty node contents
	for i, metaInfo := range metaInfo.MetaInfos {
		if metaInfo.Id != nodes.Nodes[i].Id {
			t.Error("Invalid Node Id: Expected:",
				nodes.Nodes[i].Id, " , Got: ",
				metaInfo.Id)
		}

		if metaInfo.LastUpdateTs !=
			nodes.Nodes[i].LastUpdateTs {
			t.Error("Invalid Node Id: Expected:",
				nodes.Nodes[i].Id, " , Got: ",
				metaInfo.Id)
		}
	}
}

func TestNodeValueMetaInfo(t *testing.T) {
	printTest("TestNodeValueMetaInfo")
	var nodes NodeValue
	nodes.Nodes = make([]NodeInfo, 3)

	// Test empty nodes values
	m := nodes.MetaInfo()
	metaInfo, ok := m.(NodeMetaInfoList)
	if !ok {
		t.Error("Invalid type returned for metaInfo ",
			reflect.TypeOf(metaInfo))
	}

	// check len
	if len(metaInfo.MetaInfos) != len(nodes.Nodes) {
		t.Error("MetaInfo len ", len(metaInfo.MetaInfos),
			" does not match nodes len ", len(nodes.Nodes))
	}

	// check empty node contents
	for _, metaInfo := range metaInfo.MetaInfos {
		if metaInfo.Id != 0 {
			t.Error("Invalid nodeId for null node: ", metaInfo.Id)
		}
	}

	// fill it up with values
	fillUpNodeInfo(&nodes)
	fmt.Println("\nAfter filling up the nodes")
	for _, nodeInfo := range nodes.Nodes {
		fmt.Println(nodeInfo)
	}
	verifyMetaInfo(&nodes, t)
}

func TestNodeValueDiff(t *testing.T) {
	printTest("TestNodeValueDiff")
	var node_1, node_2 NodeValue

	// Case: node_1 and node_2 both have nil nodes
	// diffs must be empty lists
	n2Nodes, n1Nodes := node_1.Diff(node_2.MetaInfo())
	n2Ids, ok := n2Nodes.Ids.([]NodeId)
	if !ok {
		t.Error("Invalid type for node ids for n2: ", reflect.TypeOf(n2Nodes.Ids))
		return
	}
	n1Ids, ok := n1Nodes.Ids.([]NodeId)
	if !ok {
		t.Error("Invalid type for node ids for n1: ", reflect.TypeOf(n1Nodes.Ids))
		return
	}
	if len(n2Ids) != len(n1Ids) && len(n2Ids) != 0 {
		t.Error("Empty diff expected, got n2: ", len(n2Ids),
			", n1: ", len(n1Ids))
	}

	// Case: node_1 has empty nodes and node_2 has nil nodes
	// n2Nodes must have len zero, n1Ids must be also be of len zero
	node_1.Nodes = make([]NodeInfo, 3)
	n2Nodes, n1Nodes = node_1.Diff(node_2.MetaInfo())
	n2Ids = n2Nodes.Ids.([]NodeId)
	n1Ids = n1Nodes.Ids.([]NodeId)
	if len(n2Ids) != len(n1Ids) && len(n2Ids) != 0 {
		t.Error("Empty diff expected, got n2: ", len(n2Ids),
			", n1: ", len(n1Ids))
	}

	// Case: fill up node_2 with 3 values, make node_1 as nil
	// n2Ids must have 3 ids, and n1Ids must be empty len slice.
	node_2.Nodes = make([]NodeInfo, 3)
	fillUpNodeInfo(&node_2)
	node_1.Nodes = nil

	m := node_2.MetaInfo()
	n2MetaInfo, ok := m.(NodeMetaInfoList)
	if !ok {
		t.Error("Invalid type returned for metaInfo ",
			reflect.TypeOf(m))
	}
	if len(n2MetaInfo.MetaInfos) == 0 {
		t.Error("Invalid len of meta info: ", len(n2MetaInfo.MetaInfos))
	}

	n2Nodes, n1Nodes = node_1.Diff(n2MetaInfo)
	n2Ids = n2Nodes.Ids.([]NodeId)
	n1Ids = n1Nodes.Ids.([]NodeId)
	if len(n1Ids) != 0 {
		t.Error("Empty diff expected, got n1: ", len(n1Ids), " ", n1Ids)
	}

	if len(n2Ids) != len(n2MetaInfo.MetaInfos) {
		t.Error("MisMatched lens for n2 and difflen: ", len(n2Ids),
			", metaInfo: ", len(n2MetaInfo.MetaInfos))
	}
	for i, id := range n2Ids {
		if n2MetaInfo.MetaInfos[i].Id != id {
			t.Error("Id mismatch meta: ", n2MetaInfo.MetaInfos[i].Id,
				", diffId: ", id)
		}
	}

	// Case: fill up node_1 with 3 values, make node_2 as nil
	// n1Ids must have 3 ids, and n2Ids must be empty len slice.
	node_1.Nodes = make([]NodeInfo, 3)
	fillUpNodeInfo(&node_1)
	node_2.Nodes = nil

	m = node_2.MetaInfo()
	n2MetaInfo, ok = m.(NodeMetaInfoList)
	if !ok {
		t.Error("Invalid type returned for metaInfo ",
			reflect.TypeOf(m))
	}
	if len(n2MetaInfo.MetaInfos) != 0 {
		t.Error("Invalid len of meta info: ", len(n2MetaInfo.MetaInfos))
	}

	n2Nodes, n1Nodes = node_1.Diff(n2MetaInfo)
	n2Ids = n2Nodes.Ids.([]NodeId)
	n1Ids = n1Nodes.Ids.([]NodeId)
	if len(n2Ids) != 0 {
		t.Error("Empty diff expected, got n2: ", len(n2Ids), " ", n2Ids)
	}

	if len(n1Ids) != len(node_1.Nodes) {
		t.Error("MisMatched lens for n1Ids: ", n1Ids,
			" and node_1: ", len(node_1.Nodes))
	}
	for i, id := range n1Ids {
		if node_1.Nodes[i].Id != id {
			t.Error("Id mismatch meta: ", node_1.Nodes[i].Id,
				", diffId: ", id)
		}
	}

	// Case: node_1 has 2 newer nodes and node_2 has 2 newer nodes
	// and 1 same nodes
	node_1.Nodes = make([]NodeInfo, 5)
	node_2.Nodes = make([]NodeInfo, 5)
	for i := 0; i < 4; i++ {
		if i%2 == 0 {
			fillUpNode(&node_1.Nodes[i], i)
			fillUpNode(&node_2.Nodes[i], i)
		} else {
			fillUpNode(&node_2.Nodes[i], i)
			fillUpNode(&node_1.Nodes[i], i)
		}
	}
	fillUpNode(&node_1.Nodes[4], 4)
	node_2.Nodes[4].Id = node_1.Nodes[4].Id
	node_2.Nodes[4].LastUpdateTs = node_1.Nodes[4].LastUpdateTs

	m = node_2.MetaInfo()
	n2MetaInfo, ok = m.(NodeMetaInfoList)
	if !ok {
		t.Error("Invalid type returned for metaInfo ",
			reflect.TypeOf(m))
	}
	if len(n2MetaInfo.MetaInfos) == 0 {
		t.Error("Invalid len of meta info: ", len(n2MetaInfo.MetaInfos))
	}

	n2Nodes, n1Nodes = node_1.Diff(n2MetaInfo)
	n2Ids = n2Nodes.Ids.([]NodeId)
	n1Ids = n1Nodes.Ids.([]NodeId)
	fmt.Println("N2: ", n2Ids, " N1: ", n1Ids)

	for i := 0; i < 5; i++ {
		fmt.Println(node_1.Nodes[i])
		fmt.Println(node_2.Nodes[i])
	}

	if len(n2Ids) == 0 || len(n1Ids) == 0 {
		t.Error("Non-Empty diff expected, got n2: ", len(n2Ids), " n1: ", len(n1Ids))
	}

	if len(n1Ids) != len(n2Ids) && len(n1Ids) != 5 {
		t.Error("MisMatched lens for n1Ids: ", n1Ids,
			" and n2Ids: ", n2Ids)
	}
	for i := 0; i < 4; i++ {
		if i%2 == 0 {
			if n2Ids[i] != NodeId(i+1) {
				t.Error("Expected n2 to be present, got: ", n2Ids[i])
			}
			if n1Ids[i] != 0 {
				t.Error("Expected n1 to be absent, got: ", n1Ids[i])
			}
		} else {
			if n1Ids[i] != NodeId(i+1) {
				t.Error("Expected n1 to be present, got: ", n1Ids[i])
			}
			if n2Ids[i] != 0 {
				t.Error("Expected n2 to be absent, got: ", n2Ids[i])
			}
		}
	}

	if n2Ids[4] != 0 || n1Ids[4] != 0 {
		t.Error("Common element wrongly passed n2: ", n2Ids[4], " n1: ", n1Ids[4])
	}

}

func verifyNodeInfoEquality(c *NodeValue, u *NodeValue, t *testing.T) {
	for i := 0; i < len(u.Nodes); i++ {
		if c.Nodes[i].Id != u.Nodes[i].Id ||
			c.Nodes[i].LastUpdateTs != u.Nodes[i].LastUpdateTs {
			t.Error("NodeInfo Mismatch: c: ", c, " u: ", u)
		}
	}
}

func TestNodeValueUpdate(t *testing.T) {
	printTest("TestNodeValueUpdate")
	curr := &NodeValue{}
	update := &NodeValue{}

	// Case: Both current node and update are nil
	curr.Update(update)
	if curr.Nodes != nil {
		t.Error("After nil updating nil, nodes are non-nil: ", curr.Nodes)
	}

	// Case: Current node is nil and there is an update
	update.Nodes = make([]NodeInfo, 3)
	fillUpNodeInfo(update)
	curr.Update(update)
	if len(curr.Nodes) != len(update.Nodes) {
		t.Error("Len mismatch after update, curr: ", len(curr.Nodes),
			" update: ", len(update.Nodes))
	}
	verifyNodeInfoEquality(curr, update, t)

	// Case: Current node is non-emtpy and update is nil
	newNilUpdate := &NodeValue{}
	curr.Update(newNilUpdate)
	if len(curr.Nodes) != len(update.Nodes) {
		t.Error("Len mismatch after update, curr: ", len(curr.Nodes),
			" update: ", len(update.Nodes))
	}
	verifyNodeInfoEquality(curr, update, t)

	// Case: Current node and update are non-nil, update being
	// shorter than current nodes len
	update.Nodes = make([]NodeInfo, 2)
	fillUpNodeInfo(update)
	sameNodeInfo := curr.Nodes[2]
	origLen := len(curr.Nodes)
	curr.Update(update)
	if len(curr.Nodes) != origLen {
		t.Error("Len mismatch after update, curr: ", len(curr.Nodes),
			" original len: ", origLen)
	}
	verifyNodeInfoEquality(curr, update, t)
	if curr.Nodes[2].Id != sameNodeInfo.Id ||
		curr.Nodes[2].LastUpdateTs != sameNodeInfo.LastUpdateTs {
		t.Error("Same NodeInfo Mismatch: c: ", curr, " u: ", sameNodeInfo)
	}

	// Case: Current node and update are non-nil, update being
	// longer than current nodes len
	update.Nodes = make([]NodeInfo, 5)
	fillUpNodeInfo(curr)
	fillUpNodeInfo(update)
	curr.Update(update)
	if len(curr.Nodes) != len(update.Nodes) {
		t.Error("Len mismatch after update, curr: ", len(curr.Nodes),
			" update len: ", len(update.Nodes))
	}
	verifyNodeInfoEquality(curr, update, t)

	// Case: Current node and update are non-nil, update being
	// older than current node contents
	update.Nodes = make([]NodeInfo, 5)
	curr.Nodes = make([]NodeInfo, 5)
	copyOfCurr := &NodeValue{}
	copyOfCurr.Nodes = make([]NodeInfo, 5)
	fillUpNodeInfo(update)
	fillUpNodeInfo(curr)
	copy(copyOfCurr.Nodes, curr.Nodes)
	curr.Update(update)
	if len(curr.Nodes) != len(update.Nodes) {
		t.Error("Len mismatch after update, curr: ", len(curr.Nodes),
			" update len: ", len(update.Nodes))
	}
	verifyNodeInfoEquality(curr, copyOfCurr, t)

	// Case: Current node and update are non-nil, update has only
	// one new element
	fillUpNodeInfo(curr)
	copy(update.Nodes, curr.Nodes)
	lastNode := len(update.Nodes) - 1
	update.Nodes[lastNode].LastUpdateTs = time.Now()
	curr.Update(update)
	copy(copyOfCurr.Nodes, curr.Nodes)
	copyOfCurr.Nodes = copyOfCurr.Nodes[:len(update.Nodes)]
	if len(curr.Nodes) != len(copyOfCurr.Nodes) {
		t.Error("Len mismatch after update, curr: ", len(curr.Nodes),
			" update len: ", len(update.Nodes))
	}
	verifyNodeInfoEquality(curr, copyOfCurr, t)
	if curr.Nodes[lastNode].Id != update.Nodes[lastNode].Id ||
		curr.Nodes[lastNode].LastUpdateTs != update.Nodes[lastNode].LastUpdateTs {
		t.Error("Same NodeInfo Mismatch: c: ", curr, " u: ", update.Nodes[lastNode])
	}

}
