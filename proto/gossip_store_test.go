package proto

import (
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/libopenstorage/gossip/types"
	"math/rand"
	"runtime"
	"strconv"
	"testing"
	"time"
)

const (
	CPU    types.StoreKey = "CPU"
	MEMORY types.StoreKey = "MEMORY"
	ID     types.NodeId   = "4"
)

func printTestInfo() {
	pc := make([]uintptr, 3) // at least 1 entry needed
	runtime.Callers(2, pc)
	f := runtime.FuncForPC(pc[0])
	fmt.Println("RUNNING TEST: ", f.Name())
}

func flipCoin() bool {
	if rand.Intn(100) < 50 {
		return true
	}
	return false
}

func fillUpNodeInfo(node *types.NodeInfo, key types.StoreKey, i int) {
	node.Id = types.NodeId(strconv.Itoa(i))
	node.LastUpdateTs = time.Now()
	node.Status = types.NODE_STATUS_UP

	node.Value = make(types.StoreMap)
	node.Value[types.StoreKey(CPU+key)] = node.Id
	node.Value[types.StoreKey(MEMORY+key)] = node.Id
}

func clearKey(nodes types.NodeInfoMap, key types.StoreKey, id int) {
	nodeId := types.NodeId(strconv.Itoa(id))
	nodeInfo := nodes[nodeId]
	delete(nodeInfo.Value, types.StoreKey(CPU+key))
	delete(nodeInfo.Value, types.StoreKey(MEMORY+key))
}

func fillUpNodeInfoMap(nodes types.NodeInfoMap, key types.StoreKey,
	numOfNodes int) {
	for i := 0; i < numOfNodes; i++ {
		var node types.NodeInfo
		fillUpNodeInfo(&node, key, i)
		nodes[node.Id] = node
	}
}

func TestGossipStoreUpdateSelf(t *testing.T) {
	printTestInfo()
	// emtpy store
	g := NewGossipStore(ID)

	id := g.NodeId()
	if id != ID {
		t.Error("Incorrect NodeId(), got: ", id,
			" expected: ", ID)
	}

	value := "string"
	key1 := types.StoreKey("key1")
	// key absent, id absent
	g.UpdateSelf(key1, value)

	nodeInfo, ok := g.nodeMap[ID]
	if !ok || nodeInfo.Value == nil {
		t.Error("UpdateSelf adding new id failed")
	} else {
		nodeValue, ok := nodeInfo.Value[key1]
		if !ok {
			t.Error("UpdateSelf adding new key failed, after update state: ",
				g.nodeMap)
		} else {
			if nodeValue != value || nodeInfo.Id != ID {
				t.Error("UpdateSelf failed, got value: ", nodeInfo.Value,
					" got: ", value)
			}
		}
	}

	// key present id present
	prevTs := time.Now()
	time.Sleep(1 * time.Second)
	value = "newValue"
	g.UpdateSelf(key1, value)
	nodeInfo = g.nodeMap[ID]
	nodeValue := nodeInfo.Value[key1]
	if !prevTs.Before(nodeInfo.LastUpdateTs) {
		t.Error("UpdateSelf failed to update timestamp, prev: ", prevTs,
			" got: ", nodeInfo)
	}
	if nodeValue != value || nodeInfo.Id != ID {
		t.Error("UpdateSelf failed, got value: ", nodeInfo,
			" got: ", value, " expected id: ", ID)
	}
}

func TestGossipStoreUpdateNodeStatuses(t *testing.T) {
	printTestInfo()

	g := NewGossipStore(ID)

	nodeLen := 10
	keyList := []types.StoreKey{"key1", "key2", "key3"}
	for _, key := range keyList {
		fillUpNodeInfoMap(g.nodeMap, key, nodeLen)
	}

	time.Sleep(3 * time.Second)
	g.UpdateNodeStatuses(6*time.Second, 24*time.Second)
	for id, nodeInfo := range g.nodeMap {
		if nodeInfo.Status == types.NODE_STATUS_DOWN {
			t.Error("Node wrongly marked down: ", nodeInfo,
				" for node: ", id)
		}
	}

	time.Sleep(3 * time.Second)
	g.UpdateNodeStatuses(5*time.Second, 20*time.Second)
	for id, nodeInfo := range g.nodeMap {
		if nodeInfo.Status != types.NODE_STATUS_DOWN &&
			id != ID {
			t.Error("Node wrongly marked up: ", nodeInfo,
				" for key: ", id)
		}
	}
}

func TestGossipStoreGetStoreKeyValue(t *testing.T) {
	printTestInfo()

	// Case: emtpy store
	// Case: key absent
	g := NewGossipStore(ID)

	keyList := []types.StoreKey{"key1", "key2"}

	nodeInfoMap := g.GetStoreKeyValue(keyList[0])
	if len(nodeInfoMap) != 0 {
		t.Error("Expected empty node info list, got: ", nodeInfoMap)
	}

	// Case: key present with nodes with holes in node ids
	fillUpNodeInfoMap(g.nodeMap, keyList[0], 6)
	if len(g.nodeMap) != 6 {
		t.Error("Failed to fillup node info map properly, got: ",
			g.nodeMap)
	}
	keyCheck := types.StoreKey(CPU + keyList[0])
	delete(g.nodeMap["0"].Value, keyCheck)
	delete(g.nodeMap["2"].Value, keyCheck)
	delete(g.nodeMap["4"].Value, keyCheck)
	nodeInfoMap = g.GetStoreKeyValue(keyCheck)
	if len(nodeInfoMap) != 3 {
		t.Error("Expected list with atleast 6 elements, got: ", nodeInfoMap)
	}

	for i := 0; i < len(nodeInfoMap); i++ {
		id := types.NodeId(strconv.Itoa(i))
		if i%2 == 0 {
			if _, ok := nodeInfoMap[id]; ok {
				t.Error("No node expected, got: ", nodeInfoMap[id])
			}
			continue
		}
		infoMap := nodeInfoMap[id].Value
		if nodeInfoMap[id].Id != id ||
			nodeInfoMap[id].Status != types.NODE_STATUS_UP ||
			infoMap[types.StoreKey(CPU+keyList[0])] != id {
			t.Error("Invalid node content received, got: ", nodeInfoMap[id])
		}
	}
}

func TestGossipStoreMetaInfo(t *testing.T) {
	printTestInfo()

	g := NewGossipStore(ID)

	// Case: store empty
	m := g.MetaInfo()
	if len(m) != 0 {
		t.Error("Empty meta info expected from empty store, got: ", m)
	}

	nodeLen := 10
	// Case: store with keys, some keys have no ids, other have ids,
	keyList := []types.StoreKey{"key1", "key2", "key3"}
	g.nodeMap = make(types.NodeInfoMap)
	for _, key := range keyList {
		fillUpNodeInfoMap(g.nodeMap, key, nodeLen)
	}

	for i, key := range keyList {
		for j := 0; j < nodeLen; j++ {
			if i%2 == 0 {
				if j%2 == 0 {
					clearKey(g.nodeMap, key, j)
				}
			} else {
				if j%2 == 1 {
					clearKey(g.nodeMap, key, j)
				}
			}
		}
	}

	m = g.MetaInfo()
	if len(m) != 10 {
		t.Error("Meta info len error, got: ", len(m), " expected: ", len(keyList))
	}
	for _, metaInfo := range m {
		if _, ok := g.nodeMap[metaInfo.Id]; !ok {
			t.Error("MetaInfo returned unexpected id ", metaInfo)
		}
	}
}

func TestGossipNodeInfoMap(t *testing.T) {
	printTestInfo()

	nodeLen := 20
	g1 := NewGossipStore(ID)
	g2 := NewGossipStore(ID)

	// Case: empty store and emtpy meta info
	g2New, g1New := g1.Diff(g2.MetaInfo())
	if len(g2New) != 0 || len(g1New) != 0 {
		t.Error("Diff of empty stores not empty, g2: ", g2,
			" g1: ", g1)
	}

	// Case: empty store and non-empty meta info
	keyList := []types.StoreKey{"key1", "key2", "key3"}
	g2.nodeMap = make(types.NodeInfoMap)
	for _, key := range keyList {
		fillUpNodeInfoMap(g2.nodeMap, key, nodeLen)
	}

	g2New, g1New = g1.Diff(g2.MetaInfo())
	if len(g2New) != nodeLen || len(g1New) != 0 {
		t.Error("Diff lens unexpected, g1New: ", len(g1New),
			", g2New: ", len(g2New), " g2: ", len(g2.nodeMap))
	}

	for _, nodeId := range g2New {
		if _, ok := g2.nodeMap[nodeId]; !ok {
			t.Error("Nodes mismatch, got ids: ", nodeId,
				", expected: ", g2.nodeMap)
		}
	}

	// Case: diff of itself should return empty
	g2New, g1New = g2.Diff(g2.MetaInfo())
	if len(g2New) != 0 || len(g1New) != 0 {
		t.Error("Diff of empty stores not empty, g2New: ", g2New,
			" g1New: ", g1New)
	}

	// Case: empty store meta info with store value
	g1New, g2New = g2.Diff(g1.MetaInfo())
	if len(g2New) != nodeLen || len(g1New) != 0 {
		t.Error("Diff lens unexpected, g1New: ", len(g1New),
			", g2New: ", len(g2New), " g2: ", nodeLen)
	}

	for _, nodeId := range g2New {
		if _, ok := g2.nodeMap[nodeId]; !ok {
			t.Error("Nodes mismatch, got ids: ", nodeId,
				", expected: ", g2.nodeMap)
		}
	}

	// Case: diff with meta info such that
	// - node info missing in one or other
	// - node info present with different timestamps
	// - node info present with same timestamps
	keyList = []types.StoreKey{"key1", "key2", "key3"}
	g1.nodeMap = make(types.NodeInfoMap)
	g2.nodeMap = make(types.NodeInfoMap)
	for _, key := range keyList {
		fillUpNodeInfoMap(g1.nodeMap, key, nodeLen)
		fillUpNodeInfoMap(g2.nodeMap, key, nodeLen)
	}
	leng1New := 0
	leng2New := 0
	for id, info := range g1.nodeMap {
		// i % 4 == 0 have same timestamps
		// i % 3 == 0 node is present in g2 (absent or invalid in g1)
		// i % 3 == 1 node is present in g1 (absent or invalid in g2)
		// i % 3 == 2 node is invalid in both
		i, _ := strconv.Atoi(string(id))
		switch {
		case i%4 == 0:
			// 0, 4, 8, 12, 16
			n, _ := g2.nodeMap[id]
			n.LastUpdateTs = info.LastUpdateTs
			g2.nodeMap[id] = n
		case i%3 == 0:
			// 3,6,9,15,18
			leng2New++
			n := g1.nodeMap[id]
			if i > (3 * nodeLen / 4) {
				n.Status = types.NODE_STATUS_INVALID
				g1.nodeMap[id] = n
			} else if i > (nodeLen / 2) {
				n.Status = types.NODE_STATUS_NEVER_GOSSIPED
				g1.nodeMap[id] = n
			} else {
				delete(g1.nodeMap, id)
			}
		case i%3 == 1:
			leng1New++
			// 1,7,10,13,19
			n := g2.nodeMap[id]
			if i > (3 * nodeLen / 4) {
				n.Status = types.NODE_STATUS_INVALID
				g2.nodeMap[id] = n
			} else if i > (nodeLen / 2) {
				n.Status = types.NODE_STATUS_NEVER_GOSSIPED
				g2.nodeMap[id] = n
			} else {
				delete(g2.nodeMap, id)
			}
		case i%3 == 2:
			// 2,5,11,14,17
			n1 := g1.nodeMap[id]
			n2 := g2.nodeMap[id]
			if i > (nodeLen / 2) {
				n1.Status = types.NODE_STATUS_INVALID
				n2.Status = types.NODE_STATUS_NEVER_GOSSIPED
			} else {
				n1.Status = types.NODE_STATUS_NEVER_GOSSIPED
				n2.Status = types.NODE_STATUS_INVALID
			}
			g1.nodeMap[id] = n1
			g2.nodeMap[id] = n2
		}
		i++
	}

	// Expected g1: 1,7,10,13,19
	// Expected g2: 3,6,9,15,18
	g1New, g2New = g2.Diff(g1.MetaInfo())
	if len(g2New) != leng2New || len(g2New) != leng1New {
		t.Error("Diff lens unexpected, g1New: ", len(g1New), "expected: ",
			leng1New, ", g2New: ", len(g2New), " expected: ", leng2New,
			" g1:", g1New, " g2:", g2New)
	}
	for _, id := range g1New {
		nodeId, _ := strconv.Atoi(string(id))
		if nodeId%3 != 1 {
			t.Error("Unexpected g1New ", nodeId)
		}
	}
	for _, id := range g2New {
		nodeId, _ := strconv.Atoi(string(id))
		if nodeId%3 != 0 {
			t.Error("Unexpected g2New ", nodeId)
		}
	}
}

func compareNodeInfo(n1 types.NodeInfo, n2 types.NodeInfo) bool {
	eq := n1.Id == n2.Id && n2.LastUpdateTs == n1.LastUpdateTs &&
		n1.Status == n2.Status
	eq = eq && (n1.Value == nil && n2.Value == nil ||
		n1.Value != nil && n2.Value != nil)
	if eq && n1.Value != nil {
		eq = (len(n1.Value) == len(n2.Value))
		if !eq {
			return false
		}
		for key, value := range n1.Value {
			value2, ok := n2.Value[key]
			if !ok {
				eq = false
			}
			if value != value2 {
				eq = false
			}
		}
	}
	return eq
}

func TestGossipStoreSubset(t *testing.T) {
	printTestInfo()

	g := NewGossipStore(ID)

	log.Info("Testing: empty store and empty nodelist")
	// empty store and empty nodelist and non-empty nodelist
	diff := make(types.StoreNodes, 0)
	sv := g.Subset(diff)
	if len(sv) != 0 {
		t.Error("Emtpy subset expected, got: ", sv)
	}

	nodeLen := 10
	for i := 0; i < nodeLen*2; i++ {
		diff = append(diff, types.NodeId(strconv.Itoa(i)))
	}

	log.Info("Testing: empty store and non-empty nodelist")
	sv = g.Subset(diff)
	if len(sv) != 0 {
		t.Error("Emtpy subset expected, got: ", sv)
	}

	// store and diff asks for 20 nodes but store
	// has only a subset of it, as well as some keys
	// it does not know about
	keyList := []types.StoreKey{"key1", "key2", "key3"}
	g.nodeMap = make(types.NodeInfoMap)
	for _, key := range keyList {
		fillUpNodeInfoMap(g.nodeMap, key, nodeLen)
	}

	diff = make(types.StoreNodes, 0)
	for i := 0; i < nodeLen; i++ {
		diff = append(diff, types.NodeId(strconv.Itoa(2*i)))
	}

	log.Info("Testing: empty store and non-empty nodelist")
	sv = g.Subset(diff)
	if len(sv) != nodeLen/2 {
		t.Error("Subset has more keys then requested: ", sv)
	}
	for id, info := range sv {
		gInfo, ok := g.nodeMap[id]
		if !ok {
			t.Error("Subset returned id which was not originally present ", id)
		}
		if !compareNodeInfo(info, gInfo) {
			t.Error("Node info does not match, d:", info, " o:", gInfo)
		}
	}

}

func dumpNodeInfo(nodeInfoMap types.NodeInfoMap, s string, t *testing.T) {
	t.Log("\nDUMPING : ", s, " : LEN: ", len(nodeInfoMap))
	for _, nodeInfo := range nodeInfoMap {
		t.Log(nodeInfo)
	}
}

func verifyNodeInfoMapEquality(store types.NodeInfoMap, diff types.NodeInfoMap,
	excludeSelf bool, t *testing.T) {
	if excludeSelf {
		if len(store)+1 != len(diff) {
			t.Error("Stores do not match ",
				" got: ", store, " expected: ", diff)
		}
	} else if len(store) != len(diff) {
		t.Error("Stores do not match ",
			" got: ", store, " expected: ", diff)
	}

	for id, info := range store {
		if excludeSelf && id == ID {
			continue
		}
		dInfo, ok := diff[id]
		if !ok {
			t.Error("Diff does not have id ", id)
			continue
		}

		if !compareNodeInfo(dInfo, info) {
			t.Error("Nodes do not match, o: ", info, " d:", dInfo)
		}
	}
}

func TestGossipStoreUpdateData(t *testing.T) {
	printTestInfo()

	g := NewGossipStore(ID)

	// empty store and empty diff
	diff := types.NodeInfoMap{}
	g.Update(diff)
	if len(g.nodeMap) != 0 {
		t.Error("Updating empty store with empty diff gave non-empty store: ",
			g.nodeMap)
	}

	// empty store and non-emtpy diff
	diff = make(types.NodeInfoMap)
	nodeLen := 5
	keyList := []types.StoreKey{"key1", "key2", "key3", "key4", "key5"}
	for _, key := range keyList {
		fillUpNodeInfoMap(types.NodeInfoMap(diff), key, nodeLen)
	}
	g.Update(diff)
	verifyNodeInfoMapEquality(types.NodeInfoMap(g.nodeMap), diff, true, t)

	for nodeId, nodeInfo := range g.nodeMap {
		// id % 4 == 0 : node id is not existing
		// id % 4 == 1 : store has old timestamp
		// id % 4 == 2 : node id is invalid
		// id % 4 == 3 : store has newer data
		id, _ := strconv.Atoi(string(nodeId))
		switch {
		case id%4 == 0:
			delete(g.nodeMap, nodeId)
		case id%4 == 1:
			olderTime := nodeInfo.LastUpdateTs.UnixNano() - 1000
			nodeInfo.LastUpdateTs = time.Unix(0, olderTime)
		case id%4 == 2:
			if id > 10 {
				nodeInfo.Status = types.NODE_STATUS_INVALID
			} else {
				nodeInfo.Status = types.NODE_STATUS_NEVER_GOSSIPED
			}
		case id%4 == 3:
			n, _ := diff[nodeId]
			olderTime := nodeInfo.LastUpdateTs.UnixNano() - 1000
			n.LastUpdateTs = time.Unix(0, olderTime)
			diff[nodeId] = n
		}
	}

	g.Update(diff)
	for nodeId, nodeInfo := range g.nodeMap {
		// id % 4 == 0 : node id is not existing
		// id % 4 == 1 : store has old timestamp
		// id % 4 == 2 : node id is invalid
		// id % 4 == 3 : store has newer data
		id, _ := strconv.Atoi(string(nodeId))
		switch {
		case id%4 != 3:
			n, _ := diff[nodeId]
			if !compareNodeInfo(n, nodeInfo) {
				t.Error("Update failed, d: ", n, " o:", nodeInfo)
			}
		case id%4 == 3:
			n, _ := diff[nodeId]
			if compareNodeInfo(n, nodeInfo) {
				t.Error("Wrongly Updated latest data d: ", n, " o: ", nodeInfo)
			}
			olderTime := n.LastUpdateTs.UnixNano() + 1000
			ts := time.Unix(0, olderTime)
			if ts != nodeInfo.LastUpdateTs {
				t.Error("Wrongly Updated latest data d: ", n, " o: ", nodeInfo)
			}
		}
	}
}

func TestGossipStoreGetStoreKeys(t *testing.T) {
	printTestInfo()

	g := NewGossipStore(ID)

	keys := g.GetStoreKeys()
	if len(keys) != 0 {
		t.Error("Emtpy store returned keys: ", keys)
	}

	nodeLen := 10
	keyList := []types.StoreKey{"key5"}
	g.nodeMap = make(types.NodeInfoMap)
	for _, key := range keyList {
		fillUpNodeInfoMap(g.nodeMap, key, nodeLen)
	}

	keys = g.GetStoreKeys()
	if len(keys) != 2*len(keyList) {
		t.Error("Storekeys length mismatch, got", len(keys),
			", expected: ", 2*len(keyList))
	}

	for _, key := range keyList {
		found := 0
		for _, retkey := range keys {
			if retkey == (CPU+key) || retkey == (MEMORY+key) {
				found++
			}
		}
		if found != 2 {
			t.Error("Key not found: ", key, " keys:", keyList)
		}
	}
}

func TestGossipStoreBlackBoxTests(t *testing.T) {
	printTestInfo()

	g1 := NewGossipStore(ID)
	g2 := NewGossipStore(ID)

	nodeLen := 3
	keyList := []types.StoreKey{"key1", "key2", "key3", "key5"}
	g1.nodeMap = make(types.NodeInfoMap)
	g2.nodeMap = make(types.NodeInfoMap)
	for i, key := range keyList {
		if i%2 == 0 {
			fillUpNodeInfoMap(g1.nodeMap, key, nodeLen)
		} else {
			fillUpNodeInfoMap(g2.nodeMap, key, nodeLen)
		}
	}

	g1New, g2New := g2.Diff(g1.MetaInfo())
	g1Subset := g1.Subset(g1New)
	g2Subset := g2.Subset(g2New)

	g1.Update(g2Subset)
	g2.Update(g1Subset)

	if len(g1.nodeMap) != len(g2.nodeMap) &&
		len(g1.nodeMap) != len(keyList) {
		t.Error("States mismatch:g1\n", g1, "\ng2\n", g2)
	}

	store := g1.nodeMap
	diff := g2.nodeMap

	for id, nodeInfo := range store {
		diffNode, ok := diff[id]
		if !ok {
			t.Error("Expected node absent in diff ", id)
			continue
		}

		if nodeInfo.Value == nil || diffNode.Value == nil {
			t.Error("NodeValues are unexpectedly nil !")
			continue
		}

		if len(nodeInfo.Value) != len(diffNode.Value) {
			t.Error("Node values are different s:", nodeInfo.Value, " d:",
				diffNode.Value)
			continue
		}

		for key, value := range nodeInfo.Value {
			diffValue, _ := diffNode.Value[key]
			if diffValue != value {
				t.Error("Values mismatch for key ", key, " s:", value,
					" d:", diffValue)
			}
		}
	}
}
