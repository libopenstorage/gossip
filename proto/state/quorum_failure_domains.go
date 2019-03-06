package state

import (
	"sync"

	"github.com/Sirupsen/logrus"
	"github.com/libopenstorage/gossip/types"
)

// failureDomainsQuorum is an implementation of Quorum that incorporates
// failure domain information to determine whether a node is in quorum
type failureDomainsQuorum struct {
	selfId    types.NodeId
	activeMap types.MetroDomainsActiveMap
	lock      sync.Mutex
}

func (f *failureDomainsQuorum) IsNodeInQuorum(localNodeInfoMap types.NodeInfoMap) bool {
	f.lock.Lock()
	defer f.lock.Unlock()

	selfNodeInfo := localNodeInfoMap[f.selfId]
	selfDomain := selfNodeInfo.MetroDomain

	if !f.isNodeActive(selfDomain) {
		// This node is a part of deactivated failure domain
		// Shoot ourselves down as we are not in quorum
		return false
	}

	totalNodesInActiveDomains := uint(0)
	upNodesInActiveDomains := uint(0)

	for _, nodeInfo := range localNodeInfoMap {
		if nodeInfo.QuorumMember {
			if f.isNodeActive(nodeInfo.MetroDomain) {
				// update the total nodes in active domain
				totalNodesInActiveDomains++
			} else {
				// node is not a part of active domain
				// do not consider in quorum calculations
				continue
			}

			if nodeInfo.Status == types.NODE_STATUS_UP ||
				nodeInfo.Status == types.NODE_STATUS_NOT_IN_QUORUM ||
				nodeInfo.Status == types.NODE_STATUS_SUSPECT_NOT_IN_QUORUM {
				upNodesInActiveDomains++
			}
		}
	}

	// Check if we are in quorum
	quorumCount := (totalNodesInActiveDomains / 2) + 1
	return upNodesInActiveDomains >= quorumCount
}

func (f *failureDomainsQuorum) isNodeActive(ipDomain string) bool {
	isActive, _ := f.activeMap[ipDomain]
	return isActive
}

func (f *failureDomainsQuorum) UpdateNumOfQuorumMembers(numOfQuorumMembers uint) {
	// no op
	return
}

func (f *failureDomainsQuorum) UpdateMetroDomainsActiveMap(activeMap types.MetroDomainsActiveMap) bool {
	f.lock.Lock()
	defer f.lock.Unlock()

	prevMap := f.activeMap
	f.activeMap = make(types.MetroDomainsActiveMap)

	var stateChanged bool
	for domain, isActive := range activeMap {
		f.activeMap[domain] = isActive
		prevState := prevMap[domain]
		if prevState != isActive {
			stateChanged = true
			// State has changed
			if isActive {
				logrus.Infof("gossip: Marking %v domain as active", domain)
			} else {
				logrus.Infof("gossip: Marking %v domain as inactive", domain)
			}
		}
	}
	return stateChanged
}

func (f *failureDomainsQuorum) Type() types.QuorumProvider {
	return types.QUORUM_PROVIDER_FAILURE_DOMAINS
}
