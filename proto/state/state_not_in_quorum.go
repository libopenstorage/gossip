package state

import (
	"github.com/libopenstorage/gossip/types"
)

type notInQuorum struct {
	nodeStatus          types.NodeStatus
	id                  types.NodeId
	numQuorumMembers    uint
	stateEvent          chan types.StateEvent
	activeFailureDomain string
}

var instanceNotInQuorum *notInQuorum

func GetNotInQuorum(
	numQuorumMembers uint,
	selfId types.NodeId,
	stateEvent chan types.StateEvent,
	activeFailureDomain string,
) State {
	return &notInQuorum{
		nodeStatus:          types.NODE_STATUS_NOT_IN_QUORUM,
		numQuorumMembers:    numQuorumMembers,
		id:                  selfId,
		stateEvent:          stateEvent,
		activeFailureDomain: activeFailureDomain,
	}
}

func (niq *notInQuorum) String() string {
	return "NODE_STATUS_NOT_IN_QUORUM"
}

func (niq *notInQuorum) NodeStatus() types.NodeStatus {
	return niq.nodeStatus
}

func (niq *notInQuorum) SelfAlive(localNodeInfoMap types.NodeInfoMap) (State, error) {
	if !isNodeInQuorum(localNodeInfoMap, niq.id, niq.numQuorumMembers, niq.activeFailureDomain) {
		return niq, nil
	} else {
		up := GetUp(niq.numQuorumMembers, niq.id, niq.stateEvent, niq.activeFailureDomain)
		return up, nil
	}
}

func (niq *notInQuorum) NodeAlive(localNodeInfoMap types.NodeInfoMap) (State, error) {
	if !isNodeInQuorum(localNodeInfoMap, niq.id, niq.numQuorumMembers, niq.activeFailureDomain) {
		return niq, nil
	} else {
		up := GetUp(niq.numQuorumMembers, niq.id, niq.stateEvent, niq.activeFailureDomain)
		return up, nil
	}
}

func (niq *notInQuorum) SelfLeave() (State, error) {
	down := GetDown(niq.numQuorumMembers, niq.id, niq.stateEvent, niq.activeFailureDomain)
	return down, nil
}

func (niq *notInQuorum) NodeLeave(
	localNodeInfoMap types.NodeInfoMap,
) (State, error) {
	return niq, nil
}

func (niq *notInQuorum) UpdateClusterSize(
	numQuorumMembers uint,
	localNodeInfoMap types.NodeInfoMap,
) (State, error) {
	niq.numQuorumMembers = numQuorumMembers
	if !isNodeInQuorum(localNodeInfoMap, niq.id, niq.numQuorumMembers, niq.activeFailureDomain) {
		return niq, nil
	} else {
		up := GetUp(niq.numQuorumMembers, niq.id, niq.stateEvent, niq.activeFailureDomain)
		return up, nil
	}
}

func (niq *notInQuorum) MarkActiveFailureDomain(
	activeFailureDomain string,
	localNodeInfoMap types.NodeInfoMap,
) (State, error) {
	niq.activeFailureDomain = activeFailureDomain
	if !isNodeInQuorum(localNodeInfoMap, niq.id, niq.numQuorumMembers, niq.activeFailureDomain) {
		return niq, nil
	} else {
		up := GetUp(niq.numQuorumMembers, niq.id, niq.stateEvent, niq.activeFailureDomain)
		return up, nil
	}
}

func (niq *notInQuorum) Timeout(
	numQuorumMembers uint,
	localNodeInfoMap types.NodeInfoMap,
) (State, error) {
	return niq, nil
}
