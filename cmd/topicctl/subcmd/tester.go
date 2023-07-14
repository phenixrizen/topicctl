package subcmd

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/phenixrizen/topicctl/pkg/apply"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var testerCmd = &cobra.Command{
	Use:     "tester",
	Short:   "tester reads or writes test events to a cluster",
	PreRunE: testerPreRun,
	RunE:    testerRun,
}

type testerCmdConfig struct {
	mode         string
	readConsumer string
	topic        string
	file         string
	writeRate    int

	shared sharedOptions
}

var testerConfig testerCmdConfig

func init() {
	testerCmd.Flags().StringVar(
		&testerConfig.mode,
		"mode",
		"writer",
		"Tester mode (one of 'reader', 'writer')",
	)
	testerCmd.Flags().StringVar(
		&testerConfig.readConsumer,
		"read-consumer",
		"test-consumer",
		"Consumer group ID for reads; if blank, no consumer group is set",
	)
	testerCmd.Flags().StringVar(
		&testerConfig.topic,
		"topic",
		"",
		"Topic to write to",
	)
	testerCmd.Flags().StringVar(
		&testerConfig.file,
		"file",
		"",
		"File to use for message body",
	)
	testerCmd.Flags().IntVar(
		&testerConfig.writeRate,
		"write-rate",
		5,
		"Approximate number of messages to write per sec",
	)

	testerCmd.MarkFlagRequired("topic")
	addSharedFlags(testerCmd, &testerConfig.shared)
	RootCmd.AddCommand(testerCmd)
}

func testerPreRun(cmd *cobra.Command, args []string) error {
	return testerConfig.shared.validate()
}

func testerRun(cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		cancel()
	}()

	switch testerConfig.mode {
	case "reader":
		return runTestReader(ctx)
	case "writer":
		return runTestWriter(ctx)
	default:
		return fmt.Errorf("Mode must be set to either 'reader' or 'writer'")
	}
}

func runTestReader(ctx context.Context) error {
	adminClient, err := testerConfig.shared.getAdminClient(ctx, nil, true)
	if err != nil {
		return err
	}
	defer adminClient.Close()
	connector := adminClient.GetConnector()

	log.Infof(
		"This will read test messages from the '%s' topic in %s using the consumer group ID '%s'",
		testerConfig.topic,
		connector.Config.BrokerAddr,
		testerConfig.readConsumer,
	)

	ok, _ := apply.Confirm("OK to continue?", false)
	if !ok {
		return errors.New("Stopping because of user response")
	}

	reader := kafka.NewReader(
		kafka.ReaderConfig{
			Brokers:     []string{connector.Config.BrokerAddr},
			GroupID:     testerConfig.readConsumer,
			Dialer:      connector.Dialer,
			Topic:       testerConfig.topic,
			MinBytes:    10e3, // 10KB
			MaxBytes:    10e6, // 10MB
			StartOffset: kafka.LastOffset,
		},
	)

	log.Info("Starting read loop")

	for {
		message, err := reader.ReadMessage(ctx)
		if err != nil {
			return err
		}
		log.Infof(
			"Message at partition %d, offset %d: %s=%s",
			message.Partition,
			message.Offset,
			string(message.Key),
			string(message.Value),
		)
	}
}

func runTestWriter(ctx context.Context) error {
	adminClient, err := testerConfig.shared.getAdminClient(ctx, nil, true)
	if err != nil {
		return err
	}
	defer adminClient.Close()
	connector := adminClient.GetConnector()

	log.Infof(
		"This will write test messages to the '%s' topic in %s at a rate of %d/sec.",
		testerConfig.topic,
		connector.Config.BrokerAddr,
		testerConfig.writeRate,
	)

	ok, _ := apply.Confirm("OK to continue?", false)
	if !ok {
		return errors.New("Stopping because of user response")
	}

	writer := kafka.NewWriter(
		kafka.WriterConfig{
			Brokers:       []string{connector.Config.BrokerAddr},
			Dialer:        connector.Dialer,
			Topic:         testerConfig.topic,
			Balancer:      &kafka.LeastBytes{},
			Async:         true,
			QueueCapacity: 5,
			BatchSize:     5,
		},
	)
	defer writer.Close()

	index := 0
	tickDuration := time.Duration(1000.0/float64(testerConfig.writeRate)) * time.Millisecond
	sendTicker := time.NewTicker(tickDuration)
	logTicker := time.NewTicker(5 * time.Second)

	// reade the file into memory
	var fileContents []byte
	if testerConfig.file != "" {
		var err error
		fileContents, err = ioutil.ReadFile(testerConfig.file)
		if err != nil {
			return err
		}
	}

	log.Info("Starting write loop")

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-sendTicker.C:
			err := writer.WriteMessages(
				ctx,
				kafka.Message{
					Key: []byte(uuid.New().String()),
					Headers: []kafka.Header{
						protocol.Header{
							Key:   "schema",
							Value: []byte("rocky-eval-schema"),
						},
						protocol.Header{
							Key:   "workspace_id",
							Value: []byte(uuid.New().String()),
						},
						protocol.Header{
							Key:   "connector_id",
							Value: []byte(uuid.New().String()),
						},
						protocol.Header{
							Key:   "claim_id",
							Value: []byte(uuid.New().String()),
						},
					},
					Value: fileContents,
				},
			)
			if err != nil {
				return err
			}
			index++
		case <-logTicker.C:
			log.Infof("%d messages sent", index)
		}
	}
}
