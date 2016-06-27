package proto

import (
	"fmt"
	"sync"
	"time"

	//"github.com/Sirupsen/logrus"
	"github.com/libopenstorage/gossip/types"
)

const (
	INVALID_GEN_NUMBER = 0
)

type GossipStoreImpl struct {
	sync.Mutex
	id            types.NodeId
	GenNumber     uint64
	nodeMap       types.NodeInfoMap
	selfCorrect   bool
	GossipVersion string
	// This cluster size is updated from an external source
	// such as a kv database. This is an extra measure to find the
	// number of nodes in the cluster other than just relying on
	// memberlist and the length of nodeMap. It is used in
	// determining the cluster quorum
	clusterSize int
	// Ts at which we lost quorum
	lostQuorumTs  time.Time
}

func NewGossipStore(id types.NodeId, version string) *GossipStoreImpl {
	n := &GossipStoreImpl{}
	n.InitStore(id, version, types.NODE_STATUS_NOT_IN_QUORUM)
	n.selfCorrect = false
	return n
}

func (s *GossipStoreImpl) NodeId() types.NodeId {
	return s.id
}

func (s *GossipStoreImpl) UpdateLostQuorumTs() {
	s.Lock()
	defer s.Unlock()

	s.lostQuorumTs = time.Now()
}

func (s *GossipStoreImpl) GetLostQuorumTs() time.Time {
	return s.lostQuorumTs
}

func (s *GossipStoreImpl) InitStore(id types.NodeId, version string, status types.NodeStatus) {
	s.nodeMap = make(types.NodeInfoMap)
	s.id = id
	s.selfCorrect = true
	s.GossipVersion = version
	nodeInfo := types.NodeInfo{
		Id:           s.id,
		GenNumber:    s.GenNumber,
		Value:        make(types.StoreMap),
		LastUpdateTs: time.Now(),
		Status:       status,
	}
	s.nodeMap[s.id] = nodeInfo
}

func (s *GossipStoreImpl) updateSelfTs() {
	s.Lock()
	defer s.Unlock()

	nodeInfo, _ := s.nodeMap[s.id]
	nodeInfo.LastUpdateTs = time.Now()
	s.nodeMap[s.id] = nodeInfo
}

func (s *GossipStoreImpl) UpdateSelf(key types.StoreKey, val interface{}) {
	s.Lock()
	defer s.Unlock()

	nodeInfo, _ := s.nodeMap[s.id]
	nodeInfo.Value[key] = val
	nodeInfo.LastUpdateTs = time.Now()
	s.nodeMap[s.id] = nodeInfo
}

func (s *GossipStoreImpl) UpdateSelfStatus(status types.NodeStatus) {
	s.Lock()
	defer s.Unlock()

	nodeInfo, _ := s.nodeMap[s.id]
	nodeInfo.Status = status
	nodeInfo.LastUpdateTs = time.Now()
	s.nodeMap[s.id] = nodeInfo
}

func (s *GossipStoreImpl) GetSelfStatus() types.NodeStatus {
	s.Lock()
	defer s.Unlock()

	nodeInfo, _ := s.nodeMap[s.id]
	return nodeInfo.Status
}

func (s *GossipStoreImpl) UpdateNodeStatus(nodeId types.NodeId, status types.NodeStatus) error {
	s.Lock()
	defer s.Unlock()

	nodeInfo, ok := s.nodeMap[nodeId]
	if !ok {
		return fmt.Errorf("Node with id (%v) not found", nodeId)
	}
	nodeInfo.Status = status
	nodeInfo.LastUpdateTs = time.Now()
	s.nodeMap[nodeId] = nodeInfo
	return nil
}

func (s *GossipStoreImpl) GetStoreKeyValue(key types.StoreKey) types.NodeValueMap {
	s.Lock()
	defer s.Unlock()

	nodeValueMap := make(types.NodeValueMap)
	for id, nodeInfo := range s.nodeMap {
		if statusValid(nodeInfo.Status) && nodeInfo.Value != nil {
			ok := len(nodeInfo.Value) == 0
			val, exists := nodeInfo.Value[key]
			if ok || exists {
				n := types.NodeValue{Id: nodeInfo.Id,
					GenNumber:    nodeInfo.GenNumber,
					LastUpdateTs: nodeInfo.LastUpdateTs,
					Status:       nodeInfo.Status}
				n.Value = val
				nodeValueMap[id] = n
			}
		}
	}
	return nodeValueMap
}

func (s *GossipStoreImpl) GetStoreKeys() []types.StoreKey {
	s.Lock()
	defer s.Unlock()

	keyMap := make(map[types.StoreKey]bool)
	for _, nodeInfo := range s.nodeMap {
		if nodeInfo.Value != nil {
			for key, _ := range nodeInfo.Value {
				keyMap[key] = true
			}
		}
	}
	storeKeys := make([]types.StoreKey, len(keyMap))
	i := 0
	for key, _ := range keyMap {
		storeKeys[i] = key
		i++
	}
	return storeKeys
}

func (s *GossipStoreImpl) GetGossipVersion() string {
	return s.GossipVersion
}

func statusValid(s types.NodeStatus) bool {
	return (s != types.NODE_STATUS_INVALID &&
		s != types.NODE_STATUS_NEVER_GOSSIPED)
}

func (s *GossipStoreImpl) NewNode(id types.NodeId) {
	s.Lock()
	if _, ok := s.nodeMap[id]; ok {
		s.Unlock()
		return
	}

	newNodeInfo := types.NodeInfo{
		Id:                 id,
		GenNumber:          0,
		LastUpdateTs:       time.Now(),
		WaitForGenUpdateTs: time.Now(),
		Status:             types.NODE_STATUS_UP,
		Value:              make(types.StoreMap),
	}
	s.nodeMap[id] = newNodeInfo
	s.Unlock()
}

func (s *GossipStoreImpl) MetaInfo() types.NodeMetaInfo {
	s.Lock()
	defer s.Unlock()

	selfNodeInfo, _ := s.nodeMap[s.id]
	nodeMetaInfo := types.NodeMetaInfo{
		Id:            selfNodeInfo.Id,
		LastUpdateTs:  selfNodeInfo.LastUpdateTs,
		GenNumber:     selfNodeInfo.GenNumber,
		GossipVersion: s.GossipVersion,
	}
	return nodeMetaInfo
}

func (s *GossipStoreImpl) GetLocalState() types.NodeInfoMap {
	s.Lock()
	defer s.Unlock()
	return s.nodeMap
}

func (s *GossipStoreImpl) GetLocalNodeInfo(id types.NodeId) (types.NodeInfo, error) {
	s.Lock()
	defer s.Unlock()

	nodeInfo, ok := s.nodeMap[id]
	if !ok {
		return types.NodeInfo{}, fmt.Errorf("Node with id (%v) not found", id)
	}
	return nodeInfo, nil
}

/*func (s *GossipStoreImpl) sendQuorumEvents() {
	if s.GetSelfStatus() == types.NODE_STATUS_UP_AND_WAITING_FOR_QUORUM {
		logrus.Infof("got in send quorum events %v", s.NodeId())
		select {
		case s.NodeEvent <- true:
			logrus.Infof("Sent an event")
		case <-time.After(5 * time.Second):
			logrus.Infof("Timeout on sending event")
		}
	} else {
		//logrus.Infof("For node %v: in send quorum events. Calling CAQ", s.NodeId())
		s.CheckAndUpdateQuorum()
	}
}

func (s *GossipStoreImpl) recvQuorumEvents() {
	for {
		allEventsHandled := false
		diffTime := time.Since(s.GetLostQuorumTs())
		if s.quorumTimeout < diffTime {
			// Ideally this should not happen. The select timeout should get triggered before
			// we reach this state.
			// This an extra check
			if s.GetSelfStatus() == types.NODE_STATUS_UP_AND_WAITING_FOR_QUORUM {
				// Our status did not change to Up
				// Change the status to waiting for quorum
				logrus.Warnf("Quorum Timeout for Node %v with status:"+
					" (UP_AND_WAITING_FOR_QUORUM). "+
					"New Status: (WAITING_FOR_QUORUM)", s.NodeId())
				s.UpdateSelfStatus(types.NODE_STATUS_WAITING_FOR_QUORUM)
			}
			break
		}
		selectTimeout := s.quorumTimeout - diffTime
		logrus.Infof("Waiting for event on channel")
		select {
		case eventOccured := <-s.NodeEvent:
			// An event has occured. Lets check the quorum again
			if eventOccured {
				logrus.Infof("Event occured Node %v with status (UP_AND_WAITING_FOR_QUORUM)", s.NodeId())
			}
			s.CheckAndUpdateQuorum()
		case <-time.After(selectTimeout):
			if s.GetSelfStatus() == types.NODE_STATUS_UP_AND_WAITING_FOR_QUORUM {
				// Out status did not change to Up
				// Change the status to waiting for quorum
				logrus.Warnf("Quorum Timeout for Node %v with status:"+
					" (UP_AND_WAITING_FOR_QUORUM). "+
					"New Status: (WAITING_FOR_QUORUM)", s.NodeId())
				s.UpdateSelfStatus(types.NODE_STATUS_WAITING_FOR_QUORUM)
			}
			// As a timeout has occured we do not need to receive any more events.
			// break the loop
			allEventsHandled = true
		}
		if allEventsHandled {
			// End the loop and the go-routine
			break
		}
	}
}

func (s *GossipStoreImpl) CheckAndUpdateQuorum() {
	clusterSize := s.GetClusterSize()

	quorum := (clusterSize / 2) + 1
	selfNodeId := s.NodeId()

	upNodes := 0
	var selfStatus types.NodeStatus

	localNodeInfoMap := s.GetLocalState()
	for _, nodeInfo := range localNodeInfoMap {
		if nodeInfo.Id == selfNodeId {
			selfStatus = nodeInfo.Status
		}
		if nodeInfo.Status == types.NODE_STATUS_UP ||
			nodeInfo.Status == types.NODE_STATUS_WAITING_FOR_QUORUM ||
			nodeInfo.Status == types.NODE_STATUS_UP_AND_WAITING_FOR_QUORUM {
			upNodes++
		}
	}

	if upNodes < quorum {
		if selfStatus == types.NODE_STATUS_DOWN {
			// We are already down. No need of updating the status based on quorum.
			return
		} else if selfStatus == types.NODE_STATUS_UP {
			// We were up, but now we have lost quorum
			logrus.Warnf("Node %v with status: (UP) lost quorum. "+
				"New Status: (UP_AND_WAITING_FOR_QUORUM)", s.NodeId())
			s.UpdateLostQuorumTs()
			go s.recvQuorumEvents()
			s.UpdateSelfStatus(types.NODE_STATUS_UP_AND_WAITING_FOR_QUORUM)
		} else {
			
			// Do nothing. Let the status remain same.
			//s.UpdateSelfStatus(types.NODE_STATUS_WAITING_FOR_QUORUM)
		}
	} else {
		if selfStatus != types.NODE_STATUS_UP {
			logrus.Infof("Node %v now in quorum. "+
				"New Status: (UP)", s.NodeId())
			s.UpdateSelfStatus(types.NODE_STATUS_UP)
		} else {
			// No need to update status, we are already up
		}
	}
}*/

func (s *GossipStoreImpl) Update(diff types.NodeInfoMap) {
	s.Lock()
	defer s.Unlock()

	for id, newNodeInfo := range diff {
		if id == s.id {
			continue
		}
		selfValue, ok := s.nodeMap[id]
		if !ok || !statusValid(selfValue.Status) ||
			selfValue.LastUpdateTs.Before(newNodeInfo.LastUpdateTs) {
			// Our view of Status of a Node, should only be determined by memberlist.
			// We should not update the Status field in our nodeInfo based on what other node's
			// value is.
			newNodeInfo.Status = selfValue.Status
			s.nodeMap[id] = newNodeInfo
		}
	}
}

func (s *GossipStoreImpl) updateClusterSize(clusterSize int) {
	s.Lock()
	s.clusterSize = clusterSize
	s.Unlock()
}

func (s *GossipStoreImpl) getClusterSize() int {
	return s.clusterSize
}
