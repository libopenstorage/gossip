package proto

import (
	"fmt"
	"math/rand"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/libopenstorage/gossip/types"
)

const (
	CPU    string       = "CPU"
	MEMORY string       = "MEMORY"
	ID     types.NodeId = "4"
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

func fillUpNodeInfo(node *types.NodeInfo, i int) {
	node.Id = types.NodeId(strconv.Itoa(i))
	node.LastUpdateTs = time.Now()
	node.Status = types.NODE_STATUS_UP

	value := make(map[string]types.NodeId)
	value[CPU] = node.Id
	value[MEMORY] = node.Id
	node.Value = value
}

func fillUpNodeInfoMap(nodes types.NodeInfoMap, numOfNodes int) {
	for i := 0; i < numOfNodes; i++ {
		var node types.NodeInfo
		fillUpNodeInfo(&node, i)
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
	// key absent
	g.UpdateSelf(key1, value)
	nodeValue, ok := g.kvMap[key1]
	if !ok {
		t.Error("UpdateSelf adding new key failed, after update state: ",
			g.kvMap)
	} else {
		nodeInfo, ok := nodeValue[ID]
		if !ok {
			t.Error("UpdateSelf adding new id failed, nodeMap: ", nodeValue)
		}
		if nodeInfo.Value != value ||
			nodeInfo.Id != ID {
			t.Error("UpdateSelf failed, got value: ", nodeInfo.Value,
				" got: ", value)
		}
	}

	// key present id absent
	delete(g.kvMap[key1], ID)
	g.UpdateSelf(key1, value)
	nodeValue = g.kvMap[key1]
	nodeInfo, ok := nodeValue[ID]
	if !ok {
		t.Error("UpdateSelf adding new id failed, nodeMap: ", nodeValue)
	}
	if nodeInfo.Value != value || nodeInfo.Id != ID {
		t.Error("UpdateSelf failed, got value: ", nodeInfo,
			" got: ", value, " expected id: ", ID)
	}

	// key present id present
	prevTs := nodeInfo.LastUpdateTs
	value = "newValue"
	g.UpdateSelf(key1, value)
	nodeValue = g.kvMap[key1]
	nodeInfo = nodeValue[ID]
	if !nodeInfo.LastUpdateTs.After(prevTs) {
		t.Error("UpdateSelf failed to update timestamp, prev: ", prevTs,
			" got: ", nodeInfo.LastUpdateTs)
	}
	if nodeInfo.Value != value || nodeInfo.Id != ID {
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
		g.kvMap[key] = make(types.NodeInfoMap)
		fillUpNodeInfoMap(g.kvMap[key], nodeLen)
	}

	time.Sleep(3 * time.Second)
	g.UpdateNodeStatuses(6 * time.Second)
	for _, key := range keyList {
		nodeMapInfo := g.kvMap[key]
		for _, nodeInfo := range nodeMapInfo {
			if nodeInfo.Status == types.NODE_STATUS_DOWN {
				t.Error("Node wrongly marked down: ", nodeInfo,
					" for key: ", key)
			}
		}
	}

	time.Sleep(3 * time.Second)
	g.UpdateNodeStatuses(5 * time.Second)
	for _, key := range keyList {
		nodeMapInfo := g.kvMap[key]
		for id, nodeInfo := range nodeMapInfo {
			if nodeInfo.Status != types.NODE_STATUS_DOWN &&
				id != ID {
				t.Error("Node wrongly marked up: ", nodeInfo,
					" for key: ", key)
			}
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
	g.kvMap[keyList[0]] = make(types.NodeInfoMap)
	g.kvMap[keyList[1]] = make(types.NodeInfoMap)

	// Case: key present but no nodes
	nodeInfoMap = g.GetStoreKeyValue(keyList[0])
	if len(nodeInfoMap) != 0 {
		t.Error("Expected empty node info list, got: ", nodeInfoMap)
	}

	// Case: key present with nodes with holes in node ids
	fillUpNodeInfoMap(g.kvMap[keyList[0]], 6)
	if len(g.kvMap[keyList[0]]) != 6 {
		t.Error("Failed to fillup node info map properly, got: ",
			g.kvMap[keyList[0]])
	}
	delete(g.kvMap[keyList[0]], "0")
	delete(g.kvMap[keyList[0]], "2")
	delete(g.kvMap[keyList[0]], "4")
	nodeInfoMap = g.GetStoreKeyValue(keyList[0])
	if len(nodeInfoMap) != 3 {
		t.Error("Expected list with atleast 6 elements, got: ", nodeInfoMap)
	}
	for i := 0; i < len(nodeInfoMap); i++ {
		id := types.NodeId(strconv.Itoa(i))
		if i%2 == 0 {
			if nodeInfoMap[id].Status != types.NODE_STATUS_INVALID {
				t.Error("Invalid node expected, got: ", nodeInfoMap[id])
			}
			continue
		}
		infoMap := nodeInfoMap[id].Value.(map[string]types.NodeId)
		if nodeInfoMap[id].Id != id ||
			nodeInfoMap[id].Status != types.NODE_STATUS_UP ||
			infoMap[CPU] != id ||
			infoMap[MEMORY] != id {
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
	for i, key := range keyList {
		g.kvMap[key] = make(types.NodeInfoMap)
		fillUpNodeInfoMap(g.kvMap[key], nodeLen)

		for j := 0; j < nodeLen; j++ {
			if i%2 == 0 {
				if j%2 == 0 {
					delete(g.kvMap[key], types.NodeId(j))
				}
			} else {
				if j%2 == 1 {
					delete(g.kvMap[key], types.NodeId(j))
				}
			}
		}
	}

	m = g.MetaInfo()
	if len(m) != 3 {
		t.Error("Meta info len error, got: ", len(m), " expected: ", len(keyList))
	}
	for key, metaInfoList := range m {
		if len(metaInfoList.List) != len(g.kvMap[key]) {
			t.Error("Unexpected meta info returned, expected: ", nodeLen/2,
				" got: ", len(metaInfoList.List))
		}

		for _, metaInfo := range metaInfoList.List {
			nodeInfo, ok := g.kvMap[key][metaInfo.Id]
			if !ok {
				t.Error("Unexpected id returned, meta info: ", metaInfo,
					" store: ", g.kvMap[key])
				continue
			}

			if nodeInfo.Id != metaInfo.Id ||
				nodeInfo.LastUpdateTs != metaInfo.LastUpdateTs {
				t.Error("MetaInfo mismatch, nodeInfo: ", nodeInfo,
					" metaInfo: ", metaInfo)
			}
		}
	}
}

func TestGossipStoreDiff(t *testing.T) {
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
	for _, key := range keyList {
		g2.kvMap[key] = make(types.NodeInfoMap)
		fillUpNodeInfoMap(g2.kvMap[key], nodeLen)
	}

	g2New, g1New = g1.Diff(g2.MetaInfo())
	if len(g2New) != len(g2.kvMap) ||
		len(g1New) != 0 {
		t.Error("Diff lens unexpected, g1New: ", len(g1New),
			", g2New: ", len(g2New), " g2: ", len(g2.kvMap))
	}

	for key, nodeIds := range g2New {
		if len(nodeIds) != len(g2.kvMap[key]) {
			t.Error("Nodes mismatch, got ids: ", nodeIds,
				", expected: ", g2.kvMap[key])
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
	if len(g2New) != len(g2.kvMap) ||
		len(g1New) != 0 {
		t.Error("Diff lens unexpected, g1New: ", len(g1New),
			", g2New: ", len(g2New), " g2: ", len(g2.kvMap))
	}

	for key, nodeIds := range g2New {
		if len(nodeIds) != len(g2.kvMap[key]) {
			t.Error("Nodes mismatch, got ids: ", nodeIds,
				", expected: ", g2.kvMap[key])
		}
	}

	// Case: diff with meta info such that
	//   - keys are absent from store
	//   - keys are present but no node ids
	//   - keys are present, some have old and some have new ts,
	//      some have new ids and some ids from meta are missing
	keyIdMap := make(map[types.StoreKey]types.NodeId)
	for i, key := range keyList {
		g2.kvMap[key] = make(types.NodeInfoMap)
		fillUpNodeInfoMap(g2.kvMap[key], nodeLen)
		g1.kvMap[key] = make(types.NodeInfoMap)
		for id, info := range g2.kvMap[key] {
			g1.kvMap[key][id] = info
		}
		if i < 2 {
			// key3 values are same
			keyIdMap[key] = types.NodeId(i)
		} else {
			continue
		}

		// g2 has newer nodes with even id
		for id, _ := range g2.kvMap[key] {
			nodeId, _ := strconv.Atoi(string(id))
			if nodeId%2 == 0 {
				if int(nodeId) < nodeLen/2 {
					nodeInfo := g2.kvMap[key][id]
					nodeInfo.LastUpdateTs = time.Now()
					g2.kvMap[key][id] = nodeInfo
				} else {
					// store have invalid node
					if int(nodeId) > (nodeLen/2 + nodeLen/4) {
						nodeInfo := g1.kvMap[key][id]
						nodeInfo.Status = types.NODE_STATUS_INVALID
						g1.kvMap[key][id] = nodeInfo
					} else {
						// store has no node
						delete(g1.kvMap[key], id)
					}
				}
			}
		}
		// g1 has newer nodes with od ids
		for id, _ := range g1.kvMap[key] {
			nodeId, _ := strconv.Atoi(string(id))
			if nodeId%2 == 1 {
				nodeInfo := g1.kvMap[key][id]
				nodeInfo.LastUpdateTs = time.Now()
				g1.kvMap[key][id] = nodeInfo
			}
		}
	}

	g2New, g1New = g1.Diff(g2.MetaInfo())
	if len(g2New) != len(g1New) || len(g2New) != 2 {
		t.Error("Diff returned more than 2 keys, g2New: ", g2New,
			" g1New: ", g1New)
	}
	for key, nodeIds := range g2New {
		_, ok := keyIdMap[key]
		if !ok {
			t.Error("Invalid key returned: ", key)
		}

		for _, id := range nodeIds {
			nodeId, _ := strconv.Atoi(string(id))
			if nodeId%2 != 0 {
				t.Error("g2New has invalid node id: ", id)
			}
		}
	}
	for key, nodeIds := range g1New {
		_, ok := keyIdMap[key]
		if !ok {
			t.Error("Invalid key returned: ", key)
		}

		for _, id := range nodeIds {
			nodeId, _ := strconv.Atoi(string(id))
			if nodeId%2 != 1 {
				t.Error("g2New has invalid node id: ", id)
			}
		}
	}
}

func TestGossipStoreSubset(t *testing.T) {
	printTestInfo()

	g := NewGossipStore(ID)

	// empty store and empty nodelist and non-empty nodelist
	diff := types.StoreNodes{}
	sv := g.Subset(diff)
	if len(sv) != 0 {
		t.Error("Emtpy subset expected, got: ", sv)
	}

	nodeLen := 10
	keyList := []types.StoreKey{"key1", "key2", "key3"}
	for _, key := range keyList {
		nodeIds := make([]types.NodeId, nodeLen*2)
		for i := 0; i < nodeLen*2; i++ {
			nodeIds[i] = types.NodeId(strconv.Itoa(i))
		}
		diff[key] = nodeIds
	}

	sv = g.Subset(diff)
	if len(sv) != 0 {
		t.Error("Emtpy subset expected, got: ", sv)
	}

	// store and diff asks for 20 nodes but store
	// has only a subset of it, as well as some keys
	// it does not know about
	for i, key := range keyList {
		if i > 1 {
			continue
		}
		g.kvMap[key] = make(types.NodeInfoMap)
		fillUpNodeInfoMap(g.kvMap[key], nodeLen)
	}

	sv = g.Subset(diff)
	if len(sv) != 2 {
		t.Error("Subset has more keys then requested: ", sv)
	}
	for i, key := range keyList {
		nodeInfoMap, ok := sv[key]
		if i > 1 {
			if ok {
				t.Error("Subset has a key not requested: ", key)
			}
			continue
		}

		if len(nodeInfoMap) != nodeLen {
			t.Error("Subset has more keys than store: ", nodeInfoMap)
		}

		storeInfoMap := g.kvMap[key]

		if len(storeInfoMap) != len(nodeInfoMap) {
			t.Error("Subset is different then expected, got: ",
				len(nodeInfoMap), " expected: ",
				len(storeInfoMap))
		}
	}

}

func dumpNodeInfo(nodeInfoMap types.NodeInfoMap, s string, t *testing.T) {
	t.Log("\nDUMPING : ", s, " : LEN: ", len(nodeInfoMap))
	for _, nodeInfo := range nodeInfoMap {
		t.Log(nodeInfo)
	}
}

func verifyNodeInfoMapEquality(store map[types.StoreKey]types.NodeInfoMap,
	diff types.StoreDiff, selfMaybeMissing bool, t *testing.T) {
	if len(store) != len(diff) {
		t.Error("Updating empty store with non-empty diff gave error,",
			" got: ", store, " expected: ", diff)
	}
	for key, nodeInfoMap := range store {
		diffNodeInfoMap, ok := diff[key]
		if !ok {
			t.Error("Unexpected key in store after update: ", key)
			continue
		}

		if len(diffNodeInfoMap) != len(nodeInfoMap) {
			missingNodeId := make([]types.NodeId, 0)
			for id, _ := range diffNodeInfoMap {
				_, ok := nodeInfoMap[id]
				if !ok {
					missingNodeId = append(missingNodeId, id)
				}
			}
			if len(missingNodeId) > 1 ||
				!(len(missingNodeId) == 1 && missingNodeId[0] == ID &&
					selfMaybeMissing) {
				t.Error("Diff and store lengths mismatch, storelen: ",
					len(nodeInfoMap), " diff len: ", len(diffNodeInfoMap),
					" for key: ", key)
				dumpNodeInfo(diffNodeInfoMap, "DIFF", t)
				dumpNodeInfo(nodeInfoMap, "DIFF", t)
			}
		}

		for id, nodeInfo := range nodeInfoMap {
			diffNodeInfo, ok := diffNodeInfoMap[id]
			if !ok {
				t.Error("Store has unexpected node id: ", id)
			}
			if diffNodeInfo.Id != nodeInfo.Id ||
				diffNodeInfo.LastUpdateTs != nodeInfo.LastUpdateTs ||
				diffNodeInfo.Status != nodeInfo.Status {
				// FIXME/ganesh: Add check for value, it be
				// implement == operator.
				t.Error("After update mismatch, diff: ", diffNodeInfo,
					", store: ", nodeInfo, "key: ", key)
			}
		}
	}
}

func copyStoreDiff(orig map[types.StoreKey]types.NodeInfoMap,
	diff types.StoreDiff) {
	for key, nodeInfoMap := range orig {
		diffNodeInfoMap := make(types.NodeInfoMap)
		for id, nodeInfo := range nodeInfoMap {
			diffNodeInfoMap[id] = nodeInfo
		}
		diff[key] = diffNodeInfoMap
	}

}

func makeNodesOld(nodeInfoMap types.NodeInfoMap, rem int, excludeId types.NodeId,
	excludeSelfId bool) {
	for id, nodeInfo := range nodeInfoMap {
		nodeId, _ := strconv.Atoi(string(id))
		if int(nodeId)%2 == rem && id != excludeId {
			if !(id == ID && excludeSelfId) {
				if flipCoin() {
					olderTime := nodeInfo.LastUpdateTs.UnixNano() - 1000
					nodeInfo.LastUpdateTs = time.Unix(0, olderTime)
				} else {
					nodeInfo.Status = types.NODE_STATUS_INVALID
				}
				nodeInfoMap[id] = nodeInfo
			}
		}
	}
}

func TestGossipStoreUpdate(t *testing.T) {
	printTestInfo()

	g := NewGossipStore(ID)

	// empty store and empty diff and non-empty diff
	diff := types.StoreDiff{}
	diff2 := make(map[types.StoreKey]types.NodeInfoMap)
	g.Update(diff)
	if len(g.kvMap) != 0 {
		t.Error("Updating empty store with empty diff gave non-empty store: ",
			g.kvMap)
	}

	nodeLen := 10
	keyList := []types.StoreKey{"key1", "key2", "key3", "key4", "key5"}
	orig := types.StoreDiff{}
	for _, key := range keyList {
		nodeInfoMap := make(types.NodeInfoMap)
		fillUpNodeInfoMap(nodeInfoMap, nodeLen)
		diff[key] = nodeInfoMap
		diff2[key] = nodeInfoMap
	}
	copyStoreDiff(diff2, orig)
	g.Update(diff)
	verifyNodeInfoMapEquality(g.kvMap, diff, false, t)

	// store and diff has values such that -
	//   - diff has new keys
	//   - diff has same keys but some ids are newer
	//   - diff has same keys and same ids but content is newer
	diff = types.StoreDiff{}
	orig = types.StoreDiff{}
	g.kvMap = make(map[types.StoreKey]types.NodeInfoMap)
	for _, key := range keyList {
		nodeInfoMap := make(types.NodeInfoMap)
		fillUpNodeInfoMap(nodeInfoMap, nodeLen)
		g.kvMap[key] = nodeInfoMap
	}
	copyStoreDiff(g.kvMap, diff)
	copyStoreDiff(g.kvMap, orig)

	// from the store delete key1
	delete(g.kvMap, keyList[0])
	// from the diff delete key4
	delete(diff, keyList[3])

	// now make the odd number ids older in store
	// even number ids newer in diff
	// nodeid ID is newer in diff
	// nodeid 5 is left unchanged
	for _, key := range keyList {
		diffNodeInfoMap, ok := diff[key]
		if ok && key != keyList[0] {
			// id == 0 is keyList[0], which we deleted from store
			// so don't modify it in the diff or else store value
			// will be diff value which is different from orig
			makeNodesOld(diffNodeInfoMap, 0, "5", false)
		}
		storeNodeInfoMap, ok := g.kvMap[key]
		if ok && key != keyList[3] {
			makeNodesOld(storeNodeInfoMap, 1, "5", true)
		}
	}

	g.Update(diff)
	verifyNodeInfoMapEquality(g.kvMap, orig, true, t)

}

func TestGossipStoreGetStoreKeys(t *testing.T) {
	printTestInfo()

	g := NewGossipStore(ID)

	keys := g.GetStoreKeys()
	if len(keys) != 0 {
		t.Error("Emtpy store returned keys: ", keys)
	}

	nodeLen := 10
	keyList := []types.StoreKey{"key1", "key2", "key3", "key4", "key5"}
	for _, key := range keyList {
		nodeInfoMap := make(types.NodeInfoMap)
		fillUpNodeInfoMap(nodeInfoMap, nodeLen)
		g.kvMap[key] = nodeInfoMap
	}

	keys = g.GetStoreKeys()
	if len(keys) != len(g.kvMap) {
		t.Error("Storekeys length mismatch, got", len(keys),
			", expected: ", len(g.kvMap))
	}
	for _, key := range keys {
		_, ok := g.kvMap[key]
		if !ok {
			t.Error("Unexpected key returned: ", key)
		}
	}

}

func TestGossipStoreBlackBoxTests(t *testing.T) {
	printTestInfo()

	g1 := NewGossipStore(ID)
	g2 := NewGossipStore(ID)

	nodeLen := 3
	keyList := []types.StoreKey{"key1", "key2", "key3", "key5"}
	for i, key := range keyList {
		nodeInfoMap := make(types.NodeInfoMap)
		fillUpNodeInfoMap(nodeInfoMap, nodeLen)
		if i%2 == 0 {
			g1.kvMap[key] = nodeInfoMap
		} else {
			g2.kvMap[key] = nodeInfoMap
		}
	}

	g1New, g2New := g2.Diff(g1.MetaInfo())
	g1Subset := g1.Subset(g1New)
	g2Subset := g2.Subset(g2New)

	g1.Update(g2Subset)
	g2.Update(g1Subset)

	if len(g1.kvMap) != len(g2.kvMap) &&
		len(g1.kvMap) != len(keyList) {
		t.Error("States mismatch:g1\n", g1, "\ng2\n", g2)
	}

	store := g1.kvMap
	diff := g2.kvMap

	for key, nodeInfoMap := range store {
		diffNodeInfoMap, ok := diff[key]
		if !ok {
			t.Error("Unexpected key in store after update: ", key)
			continue
		}

		if len(diffNodeInfoMap) != len(nodeInfoMap) {
			missingNodeId := make([]types.NodeId, 0)
			for id, _ := range diffNodeInfoMap {
				_, ok := nodeInfoMap[id]
				if !ok {
					missingNodeId = append(missingNodeId, id)
				}
			}
			if len(missingNodeId) > 1 {
				t.Error("Diff and store lengths mismatch, storelen: ",
					len(nodeInfoMap), " diff len: ", len(diffNodeInfoMap),
					" for key: ", key)
				dumpNodeInfo(diffNodeInfoMap, "DIFF", t)
				dumpNodeInfo(nodeInfoMap, "DIFF", t)
			}
		}

		for id, nodeInfo := range nodeInfoMap {
			diffNodeInfo, ok := diffNodeInfoMap[id]
			if !ok {
				t.Error("Store has unexpected node id: ", id)
			}
			if diffNodeInfo.Id != nodeInfo.Id ||
				diffNodeInfo.LastUpdateTs != nodeInfo.LastUpdateTs ||
				diffNodeInfo.Status != nodeInfo.Status {
				t.Error("After update mismatch, diff: ", diffNodeInfo,
					", store: ", nodeInfo, "key: ", key)
			}
		}
	}
}