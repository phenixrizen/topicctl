package pickers

import (
	"sort"

	"github.com/phenixrizen/topicctl/pkg/admin"
	"github.com/phenixrizen/topicctl/pkg/util"
)

// ClusterUsePicker is a picker that considers broker use across the entire cluster to break ties.
type ClusterUsePicker struct {
	brokerCountsByPosition []map[int]int
}

var _ Picker = (*ClusterUsePicker)(nil)

// NewClusterUsePicker generates a new picker from the argument brokers and topics.
func NewClusterUsePicker(
	brokers []admin.BrokerInfo,
	topics []admin.TopicInfo,
) *ClusterUsePicker {
	// Map from position -> broker -> count
	brokerCountsByPosition := []map[int]int{}
	maxReplicas := admin.MaxReplication(topics)

	for i := 0; i < maxReplicas; i++ {
		positionMap := map[int]int{}

		for _, broker := range brokers {
			positionMap[broker.ID] = 0
		}

		brokerCountsByPosition = append(
			brokerCountsByPosition,
			positionMap,
		)
	}

	for _, topic := range topics {
		for _, partition := range topic.Partitions {
			for r, replica := range partition.Replicas {
				brokerCountsByPosition[r][replica]++
			}
		}
	}

	return &ClusterUsePicker{
		brokerCountsByPosition: brokerCountsByPosition,
	}
}

// PickNew updates the replica for the argument partition and index, using the choices in
// brokerChoices.
func (c *ClusterUsePicker) PickNew(
	topic string,
	brokerChoices []int,
	curr []admin.PartitionAssignment,
	partition int,
	index int,
) error {
	return pickNewByPositionFrequency(
		topic,
		brokerChoices,
		curr,
		partition,
		index,
		c.keySorter(index, true),
	)
}

// SortRemovals sorts the argument partitions in order of priority for removing the broker
// at the argument index.
func (c *ClusterUsePicker) SortRemovals(
	topic string,
	partitionChoices []int,
	curr []admin.PartitionAssignment,
	index int,
) error {
	return sortRemovalsByPositionFrequency(
		topic,
		partitionChoices,
		curr,
		index,
		c.keySorter(index, false),
	)
}

// ScoreBroker returns an integer score for the given broker at the provided partition and index.
func (c *ClusterUsePicker) ScoreBroker(
	topic string,
	brokerID int,
	partition int,
	index int,
) int {
	return c.brokerCountsByPosition[index][brokerID]
}

func (c *ClusterUsePicker) keySorter(index int, asc bool) util.KeySorter {
	return func(input map[int]int) []int {
		keys := util.SortedKeys(input)

		sort.Slice(keys, func(a, b int) bool {
			if asc {
				return c.brokerCountsByPosition[index][keys[a]] <
					c.brokerCountsByPosition[index][keys[b]]
			}
			return c.brokerCountsByPosition[index][keys[a]] >
				c.brokerCountsByPosition[index][keys[b]]
		})

		return keys
	}
}
