package groups

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/phenixrizen/topicctl/pkg/admin"
	"github.com/phenixrizen/topicctl/pkg/util"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetGroups(t *testing.T) {
	ctx := context.Background()
	connector, err := admin.NewConnector(admin.ConnectorConfig{
		BrokerAddr: util.TestKafkaAddr(),
	})
	require.NoError(t, err)

	topicName := createTestTopic(ctx, t, connector)
	groupID := fmt.Sprintf("test-group-%s", topicName)

	reader := kafka.NewReader(
		kafka.ReaderConfig{
			Brokers:  []string{connector.Config.BrokerAddr},
			Dialer:   connector.Dialer,
			GroupID:  groupID,
			Topic:    topicName,
			MinBytes: 50,
			MaxBytes: 10000,
		},
	)

	readerCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	for i := 0; i < 8; i++ {
		_, err := reader.ReadMessage(readerCtx)
		require.NoError(t, err)
	}

	groups, err := GetGroups(ctx, connector)
	require.NoError(t, err)

	// There could be older groups in here, just ignore them
	assert.GreaterOrEqual(t, len(groups), 1)

	var match bool
	var groupCoordinator GroupCoordinator

	for _, group := range groups {
		if group.GroupID == groupID {
			groupCoordinator = group
			match = true
			break
		}
	}
	require.True(t, match)
	assert.Equal(t, 1, len(groupCoordinator.Topics))
	assert.Equal(t, topicName, groupCoordinator.Topics[0])

	groupDetails, err := GetGroupDetails(ctx, connector, groupID)
	require.NoError(t, err)
	assert.Equal(t, groupID, groupDetails.GroupID)
	assert.Equal(t, "Stable", groupDetails.State)
	assert.Equal(t, 1, len(groupDetails.Members))
	require.Equal(t, 1, len(groupDetails.Members))

	groupPartitions := groupDetails.Members[0].TopicPartitions[topicName]

	assert.ElementsMatch(
		t,
		[]int{0, 1},
		groupPartitions,
	)
}

func TestGetGroupsMultiMember(t *testing.T) {
	ctx := context.Background()
	connector, err := admin.NewConnector(admin.ConnectorConfig{
		BrokerAddr: util.TestKafkaAddr(),
	})
	require.NoError(t, err)

	topicName := createTestTopic(ctx, t, connector)
	groupID := fmt.Sprintf("test-group-%s", topicName)

	reader1 := kafka.NewReader(
		kafka.ReaderConfig{
			Brokers:  []string{connector.Config.BrokerAddr},
			Dialer:   connector.Dialer,
			GroupID:  groupID,
			Topic:    topicName,
			MinBytes: 50,
			MaxBytes: 10000,
		},
	)
	reader2 := kafka.NewReader(
		kafka.ReaderConfig{
			Brokers:  []string{connector.Config.BrokerAddr},
			Dialer:   connector.Dialer,
			GroupID:  groupID,
			Topic:    topicName,
			MinBytes: 50,
			MaxBytes: 10000,
		},
	)

	readerCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	for i := 0; i < 4; i++ {
		_, err := reader1.ReadMessage(readerCtx)
		require.NoError(t, err)
		_, err = reader2.ReadMessage(readerCtx)
		require.NoError(t, err)
	}

	groups, err := GetGroups(ctx, connector)
	require.NoError(t, err)

	// There could be older groups in here, just ignore them
	assert.GreaterOrEqual(t, len(groups), 1)

	var match bool
	var groupCoordinator GroupCoordinator

	for _, group := range groups {
		if group.GroupID == groupID {
			groupCoordinator = group
			match = true
			break
		}
	}
	require.True(t, match)
	assert.Equal(t, 1, len(groupCoordinator.Topics))
	assert.Equal(t, topicName, groupCoordinator.Topics[0])

	groupDetails, err := GetGroupDetails(ctx, connector, groupID)
	require.NoError(t, err)
	assert.Equal(t, groupID, groupDetails.GroupID)
	assert.Equal(t, "Stable", groupDetails.State)
	assert.Equal(t, 2, len(groupDetails.Members))
	require.Equal(t, 2, len(groupDetails.Members))

	groupPartitions := []int{}
	for _, member := range groupDetails.Members {
		groupPartitions = append(groupPartitions, member.TopicPartitions[topicName]...)
	}

	assert.ElementsMatch(
		t,
		[]int{0, 1},
		groupPartitions,
	)

}

func TestGetGroupsMultiMemberMultiTopic(t *testing.T) {
	ctx := context.Background()
	connector, err := admin.NewConnector(admin.ConnectorConfig{
		BrokerAddr: util.TestKafkaAddr(),
	})
	require.NoError(t, err)

	topicName1 := createTestTopic(ctx, t, connector)
	topicName2 := createTestTopic(ctx, t, connector)

	groupID := fmt.Sprintf("test-group-%s", topicName1+topicName2)

	reader1 := kafka.NewReader(
		kafka.ReaderConfig{
			Brokers:  []string{connector.Config.BrokerAddr},
			Dialer:   connector.Dialer,
			GroupID:  groupID,
			Topic:    topicName1,
			MinBytes: 50,
			MaxBytes: 10000,
		},
	)
	reader2 := kafka.NewReader(
		kafka.ReaderConfig{
			Brokers:  []string{connector.Config.BrokerAddr},
			Dialer:   connector.Dialer,
			GroupID:  groupID,
			Topic:    topicName2,
			MinBytes: 50,
			MaxBytes: 10000,
		},
	)

	readerCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	for i := 0; i < 8; i++ {
		_, err := reader1.ReadMessage(readerCtx)
		require.NoError(t, err)
		_, err = reader2.ReadMessage(readerCtx)
		require.NoError(t, err)
	}

	groups, err := GetGroups(ctx, connector)
	require.NoError(t, err)

	// There could be older groups in here, just ignore them
	assert.GreaterOrEqual(t, len(groups), 1)

	var match bool
	var groupCoordinator GroupCoordinator

	for _, group := range groups {
		if group.GroupID == groupID {
			groupCoordinator = group
			match = true
			break
		}
	}
	require.True(t, match)
	assert.Equal(t, 2, len(groupCoordinator.Topics))

	topicsList := []string{topicName1, topicName2}
	sort.Strings(topicsList)
	assert.Equal(t, topicsList, groupCoordinator.Topics)

	groupDetails, err := GetGroupDetails(ctx, connector, groupID)
	require.NoError(t, err)
	assert.Equal(t, groupID, groupDetails.GroupID)
	assert.Equal(t, "Stable", groupDetails.State)
	assert.Equal(t, 2, len(groupDetails.Members))
	require.Equal(t, 2, len(groupDetails.Members))

	groupPartitionsOfTopic1 := []int{}
	for _, member := range groupDetails.Members {
		groupPartitionsOfTopic1 = append(groupPartitionsOfTopic1, member.TopicPartitions[topicName1]...)
	}

	assert.ElementsMatch(
		t,
		[]int{0, 1},
		groupPartitionsOfTopic1,
	)

	groupPartitionsOfTopic2 := []int{}
	for _, member := range groupDetails.Members {
		groupPartitionsOfTopic2 = append(groupPartitionsOfTopic2, member.TopicPartitions[topicName2]...)
	}

	assert.ElementsMatch(
		t,
		[]int{0, 1},
		groupPartitionsOfTopic2,
	)
}

func TestGetLags(t *testing.T) {
	ctx := context.Background()
	connector, err := admin.NewConnector(admin.ConnectorConfig{
		BrokerAddr: util.TestKafkaAddr(),
	})
	require.NoError(t, err)

	topicName := createTestTopic(ctx, t, connector)
	groupID := fmt.Sprintf("test-group-%s", topicName)

	reader := kafka.NewReader(
		kafka.ReaderConfig{
			Brokers:  []string{connector.Config.BrokerAddr},
			Dialer:   connector.Dialer,
			GroupID:  groupID,
			Topic:    topicName,
			MinBytes: 50,
			MaxBytes: 10000,
		},
	)

	readerCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	for i := 0; i < 3; i++ {
		_, err := reader.ReadMessage(readerCtx)
		require.NoError(t, err)
	}

	lags, err := GetMemberLags(ctx, connector, topicName, groupID)
	require.NoError(t, err)
	require.Equal(t, 2, len(lags))

	for l, lag := range lags {
		assert.Equal(t, l, lag.Partition)
		assert.Equal(t, int64(4), lag.NewestOffset)
		assert.LessOrEqual(t, lag.MemberOffset, int64(4))
	}
}

func TestGetEarliestOrLatestOffset(t *testing.T) {
	ctx := context.Background()
	connector, err := admin.NewConnector(admin.ConnectorConfig{
		BrokerAddr: util.TestKafkaAddr(),
	})
	require.NoError(t, err)

	topicName := createTestTopic(ctx, t, connector)
	groupID := fmt.Sprintf("test-group-%s", topicName)

	reader := kafka.NewReader(
		kafka.ReaderConfig{
			Brokers:  []string{connector.Config.BrokerAddr},
			Dialer:   connector.Dialer,
			GroupID:  groupID,
			Topic:    topicName,
			MinBytes: 50,
			MaxBytes: 10000,
		},
	)

	readerCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	for i := 0; i < 8; i++ {
		_, err := reader.ReadMessage(readerCtx)
		require.NoError(t, err)
	}

	groupDetails, err := GetGroupDetails(ctx, connector, groupID)
	require.NoError(t, err)

	groupPartitions := groupDetails.Members[0].TopicPartitions[topicName]

	for _, partition := range groupPartitions {
		offset, err := GetEarliestOrLatestOffset(ctx, connector, topicName, LatestResetOffsetsStrategy, partition)
		require.NoError(t, err)
		assert.Equal(t, int64(4), offset)

		offset, err = GetEarliestOrLatestOffset(ctx, connector, topicName, EarliestResetOffsetsStrategy, partition)
		require.NoError(t, err)
		assert.Equal(t, int64(0), offset)
	}
}

func TestResetOffsets(t *testing.T) {
	ctx := context.Background()
	connector, err := admin.NewConnector(admin.ConnectorConfig{
		BrokerAddr: util.TestKafkaAddr(),
	})
	require.NoError(t, err)

	topicName := createTestTopic(ctx, t, connector)
	groupID := fmt.Sprintf("test-group-%s", topicName)

	reader := kafka.NewReader(
		kafka.ReaderConfig{
			Brokers:  []string{connector.Config.BrokerAddr},
			Dialer:   connector.Dialer,
			GroupID:  groupID,
			Topic:    topicName,
			MinBytes: 50,
			MaxBytes: 10000,
		},
	)

	readerCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	for i := 0; i < 8; i++ {
		_, err := reader.ReadMessage(readerCtx)
		require.NoError(t, err)
	}

	require.NoError(t, err)
	err = ResetOffsets(
		ctx,
		connector,
		topicName,
		groupID,
		map[int]int64{
			0: 2,
			1: 1,
		},
	)
	require.NoError(t, err)

	lags, err := GetMemberLags(ctx, connector, topicName, groupID)
	require.NoError(t, err)

	require.Equal(t, 2, len(lags))
	assert.Equal(t, int64(2), lags[0].MemberOffset)
	assert.Equal(t, int64(1), lags[1].MemberOffset)

	// latest offset of partition 0
	latestOffset, err := GetEarliestOrLatestOffset(ctx, connector, topicName, LatestResetOffsetsStrategy, 0)
	require.NoError(t, err)
	// earliest offset of partition 1
	earliestOffset, err := GetEarliestOrLatestOffset(ctx, connector, topicName, EarliestResetOffsetsStrategy, 1)
	require.NoError(t, err)

	err = ResetOffsets(
		ctx,
		connector,
		topicName,
		groupID,
		map[int]int64{
			0: latestOffset,
			1: earliestOffset,
		},
	)
	require.NoError(t, err)

	lags, err = GetMemberLags(ctx, connector, topicName, groupID)
	require.NoError(t, err)

	require.Equal(t, 2, len(lags))
	assert.Equal(t, int64(4), lags[0].MemberOffset)
	assert.Equal(t, int64(0), lags[1].MemberOffset)

}

func createTestTopic(
	ctx context.Context,
	t *testing.T,
	connector *admin.Connector,
) string {
	topicName := util.RandomString("topic-groups-", 6)
	_, err := connector.KafkaClient.CreateTopics(
		ctx,
		&kafka.CreateTopicsRequest{
			Topics: []kafka.TopicConfig{
				{
					Topic:             topicName,
					NumPartitions:     2,
					ReplicationFactor: 1,
				},
			},
		},
	)
	require.NoError(t, err)
	time.Sleep(200 * time.Millisecond)

	writer := kafka.NewWriter(
		kafka.WriterConfig{
			Brokers:   []string{connector.Config.BrokerAddr},
			Dialer:    connector.Dialer,
			Topic:     topicName,
			BatchSize: 10,
		},
	)
	defer writer.Close()

	messages := []kafka.Message{}

	for i := 0; i < 10; i++ {
		messages = append(
			messages,
			kafka.Message{
				Key:   []byte(fmt.Sprintf("key%d", i)),
				Value: []byte(fmt.Sprintf("value%d", i)),
			},
		)
	}

	err = writer.WriteMessages(ctx, messages...)
	require.NoError(t, err)

	return topicName
}
