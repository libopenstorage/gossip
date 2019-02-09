package state

import (
	"github.com/libopenstorage/gossip/types"
)

type up struct {
	nodeStatus          types.NodeStatus
	id                  types.NodeId
	numQuorumMembers    uint
	stateEvent          chan types.StateEvent
	activeFailureDomain string
}

func GetUp(
	numQuorumMembers uint,
	selfId types.NodeId,
	stateEvent chan types.StateEvent,
	activeFailureDomain string,
) State {
	return &up{
		nodeStatus:          types.NODE_STATUS_UP,
		numQuorumMembers:    numQuorumMembers,
		id:                  selfId,
		stateEvent:          stateEvent,
		activeFailureDomain: activeFailureDomain,
	}
}

func (u *up) String() string {
	return "NODE_STATUS_UP"
}

func (u *up) NodeStatus() types.NodeStatus {
	return u.nodeStatus
}

func (u *up) SelfAlive(localNodeInfoMap types.NodeInfoMap) (State, error) {
	return u, nil
}

func (u *up) NodeAlive(localNodeInfoMap types.NodeInfoMap) (State, error) {
	return u, nil
}

func (u *up) SelfLeave() (State, error) {
	down := GetDown(u.numQuorumMembers, u.id, u.stateEvent, u.activeFailureDomain)
	return down, nil
}

func isNodeInQuorum(
	localNodeInfoMap types.NodeInfoMap,
	selfId types.NodeId,
	totalNumOfQuorumMembers uint,
	activeFailureDomain string,
) bool {
	upNodes := uint(0)
	selfNodeInfo := localNodeInfoMap[selfId]
	selfDomain := selfNodeInfo.FailureDomain

	if len(activeFailureDomain) > 0 && (selfDomain != activeFailureDomain) {
		// If there is an active failure domain, shoot ourselves down
		// if we are not part of that failure domain
		return false
	}

	totalNodesInActiveDomain := 0
	upNodesInActiveDomain := 0
	for _, nodeInfo := range localNodeInfoMap {
		if nodeInfo.QuorumMember {

			if len(activeFailureDomain) > 0 &&
				(nodeInfo.FailureDomain == activeFailureDomain) {
				// update the total nodes in active domain
				totalNodesInActiveDomain++
			}

			if nodeInfo.Status == types.NODE_STATUS_UP ||
				nodeInfo.Status == types.NODE_STATUS_NOT_IN_QUORUM ||
				nodeInfo.Status == types.NODE_STATUS_SUSPECT_NOT_IN_QUORUM {
				// update the total no. of up nodes
				upNodes++

				// update the total no. of up nodes in an active domain
				if len(activeFailureDomain) > 0 &&
					(nodeInfo.FailureDomain == activeFailureDomain) {
					upNodesInActiveDomain++
				}
			}
		}
	}

	// Check if we are in quorum
	if len(activeFailureDomain) > 0 {
		quorumCount := (totalNodesInActiveDomain / 2) + 1
		if upNodesInActiveDomain >= quorumCount {
			return true
		}
	} else {
		quorumCount := (totalNumOfQuorumMembers / 2) + 1
		if upNodes >= quorumCount {
			return true
		}
	}
	// We are out of quorum
	return false
}

func (u *up) NodeLeave(localNodeInfoMap types.NodeInfoMap) (State, error) {
	if !isNodeInQuorum(localNodeInfoMap, u.id, u.numQuorumMembers, u.activeFailureDomain) {
		// Caller of this function should start a timer
		return GetSuspectNotInQuorum(u.numQuorumMembers, u.id, u.stateEvent, u.activeFailureDomain), nil
	} else {
		return u, nil
	}
}

func (u *up) UpdateClusterSize(
	numQuorumMembers uint,
	localNodeInfoMap types.NodeInfoMap,
) (State, error) {
	u.numQuorumMembers = numQuorumMembers
	if !isNodeInQuorum(localNodeInfoMap, u.id, u.numQuorumMembers, u.activeFailureDomain) {
		// Caller of this function should start a timer
		return GetSuspectNotInQuorum(u.numQuorumMembers, u.id, u.stateEvent, u.activeFailureDomain), nil
	} else {
		return u, nil
	}
}

func (u *up) MarkActiveFailureDomain(
	activeFailureDomain string,
	localNodeInfoMap types.NodeInfoMap,
) (State, error) {
	u.activeFailureDomain = activeFailureDomain
	if !isNodeInQuorum(localNodeInfoMap, u.id, u.numQuorumMembers, u.activeFailureDomain) {
		// Caller of this function should start a timer
		return GetSuspectNotInQuorum(u.numQuorumMembers, u.id, u.stateEvent, u.activeFailureDomain), nil
	} else {
		return u, nil
	}
}

func (u *up) Timeout(
	numQuorumMembers uint,
	localNodeInfoMap types.NodeInfoMap,
) (State, error) {
	return u, nil
}
