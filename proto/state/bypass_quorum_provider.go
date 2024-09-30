package state

import (
	"github.com/libopenstorage/gossip/types"
)

type bypassQuorumProvider struct {
}

// TODO (dgoel): should IsClusterInQuorum
func (d *bypassQuorumProvider) IsNodeInQuorum(localNodeInfoMap types.NodeInfoMap) bool {
	return true
}

func (d *bypassQuorumProvider) IsDomainActive(ipDomain string) bool {
	return true
}

func (d *bypassQuorumProvider) UpdateNumOfQuorumMembers(quorumMemberMap types.ClusterDomainsQuorumMembersMap) {
}

func (d *bypassQuorumProvider) UpdateClusterDomainsActiveMap(activeMap types.ClusterDomainsActiveMap) bool {
	return false
}

func (d *bypassQuorumProvider) Type() types.QuorumProvider {
	return types.QUORUM_PROVIDER_BYPASS
}
