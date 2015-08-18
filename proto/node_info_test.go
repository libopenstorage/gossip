package proto

import (
	"fmt"
	"math/rand"
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

func verifyNodeInfo(curr *NodeInfo, update *NodeInfo, t *testing.T) {
	if curr.Id != update.Id ||
		curr.LastUpdateTs != update.LastUpdateTs {
		t.Error("Same NodeInfo Mismatch: c: ", curr, " u: ", update)
	}
}

func verifyMetaInfo(nodes *NodeValue, m StoreValueMetaInfo, t *testing.T) {

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

func verifyMetaInfoForNode(nodes *NodeValue,
	t *testing.T) {
	m := nodes.MetaInfo()
	verifyMetaInfo(nodes, m, t)
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
	verifyMetaInfoForNode(&nodes, t)
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

func verifyNodeInfoEquality(c *NodeValue, u *NodeValue, exclude int, t *testing.T) {
	for i := 0; i < len(u.Nodes); i++ {
		if i != exclude && (c.Nodes[i].Id != u.Nodes[i].Id ||
			c.Nodes[i].LastUpdateTs != u.Nodes[i].LastUpdateTs) {
			t.Error("NodeInfo Mismatch: c: ", c.Nodes[i], " u: ", u.Nodes[i])
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
	verifyNodeInfoEquality(curr, update, -1, t)

	// Case: Current node is non-emtpy and update is nil
	newNilUpdate := &NodeValue{}
	curr.Update(newNilUpdate)
	if len(curr.Nodes) != len(update.Nodes) {
		t.Error("Len mismatch after update, curr: ", len(curr.Nodes),
			" update: ", len(update.Nodes))
	}
	verifyNodeInfoEquality(curr, update, -1, t)

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
	verifyNodeInfoEquality(curr, update, -1, t)
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
	verifyNodeInfoEquality(curr, update, -1, t)

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
	verifyNodeInfoEquality(curr, copyOfCurr, -1, t)

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
	verifyNodeInfoEquality(curr, copyOfCurr, -1, t)
	if curr.Nodes[lastNode].Id != update.Nodes[lastNode].Id ||
		curr.Nodes[lastNode].LastUpdateTs != update.Nodes[lastNode].LastUpdateTs {
		t.Error("Same NodeInfo Mismatch: c: ", curr, " u: ", update.Nodes[lastNode])
	}

}

func TestNodeValueDiffValue(t *testing.T) {
	printTest("TestNodeValueDiffValue")
	testLen := 6
	curr := &NodeValue{Nodes: make([]NodeInfo, testLen)}
	fillUpNodeInfo(curr)

	// Case : storevalue diff is nil, returned storevalue must be empty
	var diff StoreValueDiff
	var result *NodeValue
	result = curr.DiffValue(diff).(*NodeValue)
	if result.Nodes == nil || len(result.Nodes) != 0 {
		t.Error("Empty diff value expected, got: ", result.Nodes)
	}

	// Case : storevalue diff is non-nil but is empty, returned
	// store value must be empty
	diff.Ids = make([]NodeId, 0)
	result = curr.DiffValue(diff).(*NodeValue)
	if result.Nodes == nil || len(result.Nodes) != 0 {
		t.Error("Empty diff value expected, got: ", result.Nodes)
	}

	// Case : diff contains random elements (has node ids greated
	// than max of curr node id), store value has those
	// elements
	rand.Seed(time.Now().UnixNano())
	diffIds := make([]NodeId, testLen*2+1)
	for i := 0; i < testLen*2; i++ {
		choice := rand.Intn(testLen * 2)
		diffIds[choice] = NodeId(choice)
	}
	diff.Ids = diffIds
	result = curr.DiffValue(diff).(*NodeValue)
	if result.Nodes == nil || len(result.Nodes) == 0 {
		t.Error("Non-emtpy diff expected, got: ", result.Nodes)
	}
	for i := 0; i < testLen; i++ {
		if i < len(curr.Nodes) {
			if diffIds[i] == curr.Nodes[i].Id {
				fmt.Println("Verifying node id: ", i)
				verifyNodeInfo(&curr.Nodes[i], &result.Nodes[i], t)
			}
		} else {
			var tInval time.Time
			if result.Nodes[i].Id != 0 ||
				result.Nodes[i].LastUpdateTs != tInval {
				t.Error("Nil Info expected, got: ", result.Nodes[i])
			}
		}
	}
}

func TestNodeValueUpdateSelfValue(t *testing.T) {
	printTest("TestNodeValueUpdateSelfValue")
	testLen := 10
	curr := &NodeValue{Nodes: make([]NodeInfo, testLen)}
	fillUpNodeInfo(curr)
	copyOfCurr := &NodeValue{Nodes: make([]NodeInfo, testLen)}
	copy(copyOfCurr.Nodes, curr.Nodes)

	update := &NodeInfo{}
	for i := 0; i < testLen; i += 2 {
		update.Id = NodeId(i + 1)
		update.LastUpdateTs = time.Now()
		update.Value = "somevalue"

		curr.UpdateSelfValue(update)
		verifyNodeInfoEquality(curr, copyOfCurr, int(update.Id-1), t)
		verifyNodeInfo(&curr.Nodes[update.Id-1], update, t)
		// restore original copy
		curr = copyOfCurr
	}
}

/************** NodeValueMap Tests **********************/

func verifyAllKeysPresent(resMap map[StoreKey]bool, t *testing.T) {
	for key, value := range resMap {
		if value != true {
			// duplicate key found
			t.Error("Missing key found: ", key)
		}
	}
}

func TestValueMapGetStoreKeys(t *testing.T) {
	printTest("TestValueMapGetStoreKeys")
	n := &NodeValueMap{}
	n.kvMap = make(map[StoreKey]*NodeValue)

	var store GossipStore
	store = n

	// Case: get keys on an empty store
	keys := store.GetStoreKeys()
	if keys == nil || len(keys) != 0 {
		t.Error("Expected empty keys, got: ", keys)
	}

	// Case: get keys on a non-empty store
	testKeys := []StoreKey{"kes1", "key2", "key3"}
	resMap := make(map[StoreKey]bool)
	for i := 0; i < len(testKeys); i++ {
		n.kvMap[testKeys[i]] = &NodeValue{}
		resMap[testKeys[i]] = false
	}
	keys = store.GetStoreKeys()
	if keys == nil || len(keys) != len(testKeys) {
		t.Error("Keys len mismatch, got: ", keys, " expected: ", testKeys)
	}
	for _, retKey := range keys {
		_, ok := resMap[retKey]
		if !ok {
			t.Error("Invalid key returned, key: ", retKey,
				", expected set: ", testKeys,
				", returned set: ", keys)
		}
		if resMap[retKey] == true {
			// duplicate key found
			t.Error("Duplicate key found for key: ", retKey)
		} else {
			resMap[retKey] = true
		}
	}
	verifyAllKeysPresent(resMap, t)

}

func TestValueMapMetaInfo(t *testing.T) {
	printTest("TestValueMapMetaInfo")
	n := &NodeValueMap{}
	n.kvMap = make(map[StoreKey]*NodeValue)

	var store GossipStore
	store = n

	// Case: get meta info on empty store
	metaInfoMap := store.MetaInfo()
	if len(metaInfoMap) != 0 {
		t.Error("Empty meta info expected, got: ", metaInfoMap)
	}

	// Case: update node and check meta info is updated
	testKeys := []StoreKey{"kes1", "key2", "key3"}
	resMap := make(map[StoreKey]bool)
	for i := 0; i < len(testKeys); i++ {
		nodes := new(NodeValue)
		nodes.Nodes = make([]NodeInfo, 3)
		fillUpNodeInfo(nodes)
		n.kvMap[testKeys[i]] = nodes
	}
	metaInfoMap = store.MetaInfo()
	if len(metaInfoMap) != len(testKeys) {
		t.Error("Mismatched metaInfo for map: ", metaInfoMap)
	}
	// check the meta info returned for each key
	for key, metaInfo := range metaInfoMap {
		verifyMetaInfo(n.kvMap[key], metaInfo, t)
		resMap[key] = true
	}
	verifyAllKeysPresent(resMap, t)
}

func TestValueMapSubset(t *testing.T) {
	printTest("TestValueMapSubset")
	n := &NodeValueMap{}
	n.kvMap = make(map[StoreKey]*NodeValue)

	var store GossipStore
	store = n

	// Case: try to get a subset for empty input
	idMap := make(StoreValueIdInfoMap)
	subset := store.Subset(idMap)
	if subset == nil || len(subset) != 0 {
		t.Error("Expected empty subset, got: ", subset)
	}

	// Case: get a subset on with some keys present
	// and some keys absent
	testKeys := []StoreKey{"kes1", "key2", "key3", "key4"}
	resMap := make(map[StoreKey]bool)
	absentKeysMap := make(map[StoreKey]bool)

	nodeLen := 10
	absentKeysFrom := 3
	for i := 0; i < len(testKeys); i++ {
		nodes := new(NodeValue)
		nodes.Nodes = make([]NodeInfo, nodeLen)
		fillUpNodeInfo(nodes)
		if i < absentKeysFrom {
			n.kvMap[testKeys[i]] = nodes
		} else {
			absentKeysMap[testKeys[i]] = true
		}

		reqIds := make([]NodeId, nodeLen+5) // +5 for testing non-existing data
		for j := 0; j < len(reqIds); j++ {
			if i%2 == 0 && j%2 == 0 {
				if j < nodeLen {
					reqIds[j] = nodes.Nodes[j].Id
				} else {
					reqIds[j] = NodeId(j)
				}
				continue
			}
			if i%2 == 1 && j%2 == 1 {
				if j < nodeLen {
					reqIds[j] = nodes.Nodes[j].Id
				} else {
					reqIds[j] = NodeId(j)
				}
				continue
			}
		}
		idMap[testKeys[i]] = StoreValueDiff{Ids: reqIds}
	}
	subset = store.Subset(idMap)

	for key, value := range subset {
		// test that the key must not be present
		if _, ok := absentKeysMap[key]; ok == true {
			t.Error("Subset returned unexpected key: ", key)
			continue
		}
		resMap[key] = true
		nodeValue, ok := value.(*NodeValue)
		if !ok {
			t.Error("Invalid type returned for nodeValue: ", reflect.TypeOf(value))
			continue
		}

		// in this node value, we will check that keys
		// requested are present, and those not requested are absent
		reqIds := idMap[key].Ids.([]NodeId)
		if len(reqIds) != len(nodeValue.Nodes) {
			t.Error("Nodes absent, requested: ", reqIds,
				" got: ", nodeValue.Nodes)
			continue
		}
		for i, reqId := range reqIds {
			if i >= nodeLen {
				if nodeValue.Nodes[i].Id != 0 {
					t.Error("Unexpected subset value: ", nodeValue.Nodes[i].Id)
				}
				continue
			}
			if reqId != nodeValue.Nodes[i].Id {
				t.Error("Unexpected subset value: ", reqId,
					" got: ", nodeValue.Nodes[i].Id)
			}
			if reqId > 0 {
				verifyNodeInfo(&n.kvMap[key].Nodes[i], &nodeValue.Nodes[i], t)
			}
		}
	}
	// check any missing keys
	for key, value := range resMap {
		if value {
			continue
		}
		if _, ok := absentKeysMap[key]; ok == false {
			t.Error("Subset has missing data for key: ", key)
		}
	}
}
