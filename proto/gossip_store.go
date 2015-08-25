package proto

import (
	log "github.com/Sirupsen/logrus"
	"sync"
	"time"

	"github.com/libopenstorage/gossip/api"
)

type NodeInfoMap map[api.NodeId]api.NodeInfo

type GossipStoreImpl struct {
	sync.Mutex
	id    api.NodeId
	kvMap map[api.StoreKey]NodeInfoMap
}

func NewGossipStore(id api.NodeId) api.GossipStore {
	n := &GossipStoreImpl{}
	n.Init(id)
	return n
}

func (s *GossipStoreImpl) NodeId() api.NodeId {
	return s.id
}

func (s *GossipStoreImpl) Init(id api.NodeId) {
	s.kvMap = make(map[api.StoreKey]NodeInfoMap)
	s.id = id
}

func (s *GossipStoreImpl) UpdateSelf(key api.StoreKey, val interface{}) {
	s.Lock()
	defer s.Unlock()

	nodeValue, ok := s.kvMap[key]
	if !ok {
		nodeValue = make(NodeInfoMap)
		s.kvMap[key] = nodeValue
	}

	nodeValue[s.id] = api.NodeInfo{Id: s.id,
		Value:        val,
		LastUpdateTs: time.Now(),
		Status:       api.NODE_STATUS_UP}
}

func (s *GossipStoreImpl) GetStoreKeyValue(key api.StoreKey) api.NodeInfoList {
	s.Lock()
	defer s.Unlock()

	// we return an array, indexed by the node id.
	// Find the max node id.
	nodeInfos, ok := s.kvMap[key]
	if !ok || len(nodeInfos) == 0 {
		return api.NodeInfoList{List: nil}
	}

	maxId := api.NodeId(0)
	for id, _ := range nodeInfos {
		if nodeInfos[id].Status == api.NODE_STATUS_INVALID {
			continue
		}
		if id > maxId {
			maxId = id
		}
	}

	// maxId + 1 because we have a zero-based indexing
	nodeInfoList := make([]api.NodeInfo, maxId+1)
	for id, _ := range nodeInfos {
		if nodeInfos[id].Status == api.NODE_STATUS_INVALID {
			continue
		}
		// this must create a copy
		nodeInfoList[id] = nodeInfos[id]
	}

	return api.NodeInfoList{List: nodeInfoList}
}

func (s *GossipStoreImpl) GetStoreKeys() []api.StoreKey {
	s.Lock()
	defer s.Unlock()

	storeKeys := make([]api.StoreKey, len(s.kvMap))
	i := 0
	for key, _ := range s.kvMap {
		storeKeys[i] = key
		i++
	}
	return storeKeys
}

func (s *GossipStoreImpl) MetaInfo() api.StoreMetaInfo {
	s.Lock()
	defer s.Unlock()

	mInfo := make(api.StoreMetaInfo, len(s.kvMap))

	for key, nodeValue := range s.kvMap {
		metaInfoList := make([]api.NodeMetaInfo, 0, len(nodeValue))

		for key, _ := range nodeValue {
			if nodeValue[key].Status != api.NODE_STATUS_INVALID {
				nodeMetaInfo := api.NodeMetaInfo{
					Id:           nodeValue[key].Id,
					LastUpdateTs: nodeValue[key].LastUpdateTs}
				metaInfoList = append(metaInfoList, nodeMetaInfo)
			}
		}

		if len(metaInfoList) > 0 {
			mInfo[key] = api.NodeMetaInfoList{List: metaInfoList}
		}
	}

	return mInfo
}

func (s *GossipStoreImpl) Diff(
	d api.StoreMetaInfo) (api.StoreNodes, api.StoreNodes) {
	s.Lock()
	defer s.Unlock()

	diffNewNodes := make(map[api.StoreKey][]api.NodeId)
	selfNewNodes := make(map[api.StoreKey][]api.NodeId)

	for key, metaInfoList := range d {
		selfNodeInfo, ok := s.kvMap[key]

		metaInfoLen := len(metaInfoList.List)
		if !ok {
			// we do not have info about this key
			newIds := make([]api.NodeId, metaInfoLen)
			for i := 0; i < metaInfoLen; i++ {
				newIds[i] = metaInfoList.List[i].Id
			}
			diffNewNodes[key] = newIds
			// nothing to add in selfNewNodes
			continue
		}

		diffNewIds := make([]api.NodeId, 0, metaInfoLen)
		selfNewIds := make([]api.NodeId, 0, metaInfoLen)
		for i := 0; i < metaInfoLen; i++ {
			metaId := metaInfoList.List[i].Id
			_, ok := selfNodeInfo[metaId]
			switch {
			case !ok:
				diffNewIds = append(diffNewIds, metaId)

			// avoid copying the whole node info
			// the diff has newer node if our status for node is invalid
			case selfNodeInfo[metaId].Status ==
				api.NODE_STATUS_INVALID:
				diffNewIds = append(diffNewIds, metaId)

			// or if its last update timestamp is newer than ours
			case selfNodeInfo[metaId].LastUpdateTs.Before(
				metaInfoList.List[i].LastUpdateTs):
				diffNewIds = append(diffNewIds, metaId)

			case selfNodeInfo[metaId].LastUpdateTs.After(
				metaInfoList.List[i].LastUpdateTs):
				selfNewIds = append(selfNewIds, metaId)
			}
		}

		if len(diffNewIds) > 0 {
			diffNewNodes[key] = diffNewIds
		}
		if len(selfNewIds) > 0 {
			selfNewNodes[key] = selfNewIds
		}
	}

	// go over keys present with us but not in the meta info
	for key, nodeInfoMap := range s.kvMap {
		_, ok := d[key]
		if ok {
			// we have handled this case above
			continue
		}

		// we do not have info about this key
		newIds := make([]api.NodeId, 0)
		for nodeId, _ := range nodeInfoMap {
			if nodeInfoMap[nodeId].Status != api.NODE_STATUS_INVALID {
				newIds = append(newIds, nodeId)
			}
		}
		selfNewNodes[key] = newIds
	}

	return diffNewNodes, selfNewNodes
}

func (s *GossipStoreImpl) Subset(nodes api.StoreNodes) api.StoreDiff {
	s.Lock()
	defer s.Unlock()

	subset := make(api.StoreDiff)

	for key, nodeIdList := range nodes {
		selfNodeInfos, ok := s.kvMap[key]
		if !ok {
			log.Info("No subset for key ", key)
			continue
		}

		// create a new map to hold the diff
		nodeInfoMap := make(NodeInfoMap)
		for _, id := range nodeIdList {
			_, ok := selfNodeInfos[id]
			if !ok {
				log.Info("Id missing from store, id: ", id, " for key: ", key)
				continue
			}
			nodeInfoMap[id] = selfNodeInfos[id]
		}
		// put it in the subset
		subset[key] = nodeInfoMap
	}

	return subset
}

func (s *GossipStoreImpl) Update(diff api.StoreDiff) {
	s.Lock()
	defer s.Unlock()

	for key, newValue := range diff {

		// XXX/gsangle: delete updates for self node, will this ever happen
		// given that we always have the most updated info ?
		delete(newValue, s.id)

		selfValue, ok := s.kvMap[key]
		if !ok {
			// create a copy
			nodeInfoMap := make(NodeInfoMap)
			for id, _ := range newValue {
				nodeInfoMap[id] = newValue[id]
			}
			s.kvMap[key] = nodeInfoMap
			continue
		}
		for id, info := range newValue {
			if selfValue[id].Status == api.NODE_STATUS_INVALID ||
				selfValue[id].LastUpdateTs.Before(info.LastUpdateTs) {
				selfValue[id] = info
			}
		}
	}
}

func (s *GossipStoreImpl) UpdateNodeStatuses(d time.Duration) {
	s.Lock()
	defer s.Unlock()

	for _, nodeValue := range s.kvMap {
		for id, _ := range nodeValue {
			if nodeValue[id].Status != api.NODE_STATUS_INVALID &&
				id != s.id &&
				(time.Now().Sub(nodeValue[id].LastUpdateTs)) >= d {
				nodeInfo := nodeValue[id]
				nodeInfo.Status = api.NODE_STATUS_DOWN
				nodeValue[id] = nodeInfo
			}
		}
	}
}
