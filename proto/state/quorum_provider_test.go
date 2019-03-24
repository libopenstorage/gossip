package state

import (
	"testing"

	"github.com/libopenstorage/gossip/types"
	"github.com/stretchr/testify/require"
)

var zones = []string{"zone0", "zone1", "zone2"}
var nodes = []string{"n0", "n1", "n2", "n3", "n4", "n5"}

func getDefaultNodeInfoMap(withNonQuorumMembers bool) types.NodeInfoMap {
	nodeInfoMap := make(types.NodeInfoMap)
	for i, _ := range nodes {
		zone := zones[0]
		if i%3 == 1 {
			zone = zones[1]
		} else if i%3 == 2 {
			zone = zones[2]
		}
		quorumMember := true
		if withNonQuorumMembers && i%2 == 0 {
			quorumMember = false
		}
		nodeInfoMap[types.NodeId(nodes[i])] = types.NodeInfo{
			ClusterDomain: zone,
			QuorumMember:  quorumMember,
			Status:        types.NODE_STATUS_UP,
		}
	}
	return nodeInfoMap
}

func TestQuorumProviderAllNodesUp(t *testing.T) {
	// All zones active
	// All nodes online
	// All nodes quorum members
	localNodeInfoMap := getDefaultNodeInfoMap(false)
	for i, _ := range nodes {
		selfId := types.NodeId(nodes[i])
		q := NewQuorumProvider(selfId, types.QUORUM_PROVIDER_FAILURE_DOMAINS)
		q.UpdateNumOfQuorumMembers(
			types.ClusterDomainsQuorumMembersMap(
				map[string]int{
					zones[0]: 2,
					zones[1]: 2,
					zones[2]: 2,
				},
			),
		)
		q.UpdateClusterDomainsActiveMap(
			types.ClusterDomainsActiveMap(
				map[string]types.ClusterDomainState{
					zones[0]: types.CLUSTER_DOMAIN_STATE_ACTIVE,
					zones[1]: types.CLUSTER_DOMAIN_STATE_ACTIVE,
					zones[2]: types.CLUSTER_DOMAIN_STATE_ACTIVE,
				},
			),
		)
		require.True(t, q.IsNodeInQuorum(localNodeInfoMap), "Expected node in quorum")
	}
}

func TestQuorumProviderOneZoneDeactivated(t *testing.T) {
	// One zone deactivated
	// All nodes online
	// All nodes quorum members
	localNodeInfoMap := getDefaultNodeInfoMap(false)
	for i, _ := range nodes {
		selfId := types.NodeId(nodes[i])
		q := NewQuorumProvider(selfId, types.QUORUM_PROVIDER_FAILURE_DOMAINS)
		q.UpdateNumOfQuorumMembers(
			types.ClusterDomainsQuorumMembersMap(
				map[string]int{
					zones[0]: 2,
					zones[1]: 2,
					zones[2]: 2,
				},
			),
		)
		q.UpdateClusterDomainsActiveMap(
			types.ClusterDomainsActiveMap(
				map[string]types.ClusterDomainState{
					zones[0]: types.CLUSTER_DOMAIN_STATE_INACTIVE,
					zones[1]: types.CLUSTER_DOMAIN_STATE_ACTIVE,
					zones[2]: types.CLUSTER_DOMAIN_STATE_ACTIVE,
				},
			),
		)
		if i%3 == 0 {
			require.False(t, q.IsNodeInQuorum(localNodeInfoMap), "Expected node not in quorum")
		} else {
			require.True(t, q.IsNodeInQuorum(localNodeInfoMap), "Expected node in quorum")
		}
	}
}

func TestQuorumProviderTwoZonesDeactivated(t *testing.T) {
	// Two zones deactivated
	// All nodes online
	// All nodes quorum members
	localNodeInfoMap := getDefaultNodeInfoMap(false)
	for i, _ := range nodes {
		selfId := types.NodeId(nodes[i])
		q := NewQuorumProvider(selfId, types.QUORUM_PROVIDER_FAILURE_DOMAINS)
		q.UpdateNumOfQuorumMembers(
			types.ClusterDomainsQuorumMembersMap(
				map[string]int{
					zones[0]: 2,
					zones[1]: 2,
					zones[2]: 2,
				},
			),
		)
		q.UpdateClusterDomainsActiveMap(
			types.ClusterDomainsActiveMap(
				map[string]types.ClusterDomainState{
					zones[0]: types.CLUSTER_DOMAIN_STATE_INACTIVE,
					zones[1]: types.CLUSTER_DOMAIN_STATE_INACTIVE,
					zones[2]: types.CLUSTER_DOMAIN_STATE_ACTIVE,
				},
			),
		)
		if i%3 == 2 {
			require.True(t, q.IsNodeInQuorum(localNodeInfoMap), "Expected node in quorum")
		} else {
			require.False(t, q.IsNodeInQuorum(localNodeInfoMap), "Expected node not in quorum")
		}
	}
}

func TestQuorumProviderOneZoneDeactivatedOneNodeOffline(t *testing.T) {
	// One zone deactivated
	// One node from active zone online
	// All quorum members
	localNodeInfoMap := getDefaultNodeInfoMap(false)

	// Keep node 0 as offline
	nodeInfo := localNodeInfoMap[types.NodeId(nodes[0])]
	nodeInfo.Status = types.NODE_STATUS_DOWN
	localNodeInfoMap[types.NodeId(nodes[0])] = nodeInfo

	for i, _ := range nodes {
		if i == 0 {
			// Node is offline
			continue
		}
		selfId := types.NodeId(nodes[i])
		q := NewQuorumProvider(selfId, types.QUORUM_PROVIDER_FAILURE_DOMAINS)
		q.UpdateNumOfQuorumMembers(
			types.ClusterDomainsQuorumMembersMap(
				map[string]int{
					zones[0]: 2,
					zones[1]: 2,
					zones[2]: 2,
				},
			),
		)
		q.UpdateClusterDomainsActiveMap(
			types.ClusterDomainsActiveMap(
				map[string]types.ClusterDomainState{
					zones[0]: types.CLUSTER_DOMAIN_STATE_ACTIVE,
					zones[1]: types.CLUSTER_DOMAIN_STATE_ACTIVE,
					zones[2]: types.CLUSTER_DOMAIN_STATE_INACTIVE,
				},
			),
		)
		if i%3 == 2 {
			require.False(t, q.IsNodeInQuorum(localNodeInfoMap), "Expected node not in quorum")
		} else {
			require.True(t, q.IsNodeInQuorum(localNodeInfoMap), "Expected node in quorum")
		}
	}
}

func TestQuorumProviderOneZoneDeactivatedQuorumNodesOffline(t *testing.T) {
	// One zone deactivated
	// 2 out of 4 nodes from active zones are offline
	// All quorum members
	localNodeInfoMap := getDefaultNodeInfoMap(false)

	nodeInfo := localNodeInfoMap[types.NodeId(nodes[0])]
	nodeInfo.Status = types.NODE_STATUS_DOWN
	localNodeInfoMap[types.NodeId(nodes[0])] = nodeInfo

	nodeInfo = localNodeInfoMap[types.NodeId(nodes[1])]
	nodeInfo.Status = types.NODE_STATUS_DOWN
	localNodeInfoMap[types.NodeId(nodes[1])] = nodeInfo

	for i, _ := range nodes {
		if i == 0 || i == 2 {
			// Node is offline
			continue
		}
		selfId := types.NodeId(nodes[i])
		q := NewQuorumProvider(selfId, types.QUORUM_PROVIDER_FAILURE_DOMAINS)
		q.UpdateNumOfQuorumMembers(
			types.ClusterDomainsQuorumMembersMap(
				map[string]int{
					zones[0]: 2,
					zones[1]: 2,
					zones[2]: 2,
				},
			),
		)

		q.UpdateClusterDomainsActiveMap(
			types.ClusterDomainsActiveMap(
				map[string]types.ClusterDomainState{
					zones[0]: types.CLUSTER_DOMAIN_STATE_ACTIVE,
					zones[1]: types.CLUSTER_DOMAIN_STATE_ACTIVE,
					zones[2]: types.CLUSTER_DOMAIN_STATE_INACTIVE,
				},
			),
		)

		// All nodes should be out of quorum
		require.False(t, q.IsNodeInQuorum(localNodeInfoMap), "Expected node %v not in quorum", i)
	}
}

func TestQuorumProviderOneZoneDeactivatedWithNonQuorumMembers(t *testing.T) {
	// One zone deactivated
	// No node offline
	// one node in each zone is non quorum member
	localNodeInfoMap := getDefaultNodeInfoMap(true)
	for i, _ := range nodes {
		selfId := types.NodeId(nodes[i])
		q := NewQuorumProvider(selfId, types.QUORUM_PROVIDER_FAILURE_DOMAINS)
		q.UpdateNumOfQuorumMembers(
			types.ClusterDomainsQuorumMembersMap(
				map[string]int{
					zones[0]: 1,
					zones[1]: 1,
					zones[2]: 1,
				},
			),
		)
		q.UpdateClusterDomainsActiveMap(
			types.ClusterDomainsActiveMap(
				map[string]types.ClusterDomainState{
					zones[0]: types.CLUSTER_DOMAIN_STATE_INACTIVE,
					zones[1]: types.CLUSTER_DOMAIN_STATE_ACTIVE,
					zones[2]: types.CLUSTER_DOMAIN_STATE_ACTIVE,
				},
			),
		)
		if i%3 == 0 {
			require.False(t, q.IsNodeInQuorum(localNodeInfoMap), "Expected node not in quorum")
		} else {
			require.True(t, q.IsNodeInQuorum(localNodeInfoMap), "Expected node in quorum")
		}
	}
}

func TestQuorumProviderOneZoneDeactivatedOneNodeOfflineWithNonQuorumMembers(t *testing.T) {
	// One zone deactivated
	// one node offline
	// one node in each zone is non quorum member
	localNodeInfoMap := getDefaultNodeInfoMap(true)
	// Keep node 5 as offline
	nodeInfo := localNodeInfoMap[types.NodeId(nodes[5])]
	nodeInfo.Status = types.NODE_STATUS_DOWN
	localNodeInfoMap[types.NodeId(nodes[5])] = nodeInfo

	for i, _ := range nodes {
		if i == 5 {
			// Node is offline
			continue
		}

		selfId := types.NodeId(nodes[i])
		q := NewQuorumProvider(selfId, types.QUORUM_PROVIDER_FAILURE_DOMAINS)
		q.UpdateNumOfQuorumMembers(
			types.ClusterDomainsQuorumMembersMap(
				map[string]int{
					zones[0]: 1,
					zones[1]: 1,
					zones[2]: 1,
				},
			),
		)
		q.UpdateClusterDomainsActiveMap(
			types.ClusterDomainsActiveMap(
				map[string]types.ClusterDomainState{
					zones[0]: types.CLUSTER_DOMAIN_STATE_INACTIVE,
					zones[1]: types.CLUSTER_DOMAIN_STATE_ACTIVE,
					zones[2]: types.CLUSTER_DOMAIN_STATE_ACTIVE,
				},
			),
		)
		require.False(t, q.IsNodeInQuorum(localNodeInfoMap), "Expected node not in quorum")
	}
}

func TestQuorumProviderNodesNeverGossiped(t *testing.T) {
	// 14 nodes in the cluster
	// 6 nodes gossiped
	// 8 nodes never gossiped
	localNodeInfoMap := getDefaultNodeInfoMap(false)
	// Loop over the 6 nodes
	for i, _ := range nodes {
		selfId := types.NodeId(nodes[i])
		q := NewQuorumProvider(selfId, types.QUORUM_PROVIDER_FAILURE_DOMAINS)
		q.UpdateNumOfQuorumMembers(
			types.ClusterDomainsQuorumMembersMap(
				map[string]int{
					zones[0]: 14,
					zones[1]: 14,
					zones[2]: 14,
				},
			),
		)
		q.UpdateClusterDomainsActiveMap(
			types.ClusterDomainsActiveMap(
				map[string]types.ClusterDomainState{
					zones[0]: types.CLUSTER_DOMAIN_STATE_ACTIVE,
					zones[1]: types.CLUSTER_DOMAIN_STATE_ACTIVE,
					zones[2]: types.CLUSTER_DOMAIN_STATE_ACTIVE,
				},
			),
		)
		require.False(t, q.IsNodeInQuorum(localNodeInfoMap), "Expected node in quorum")
	}
}
