package main

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/protobuf"
)

type ConsumerHandler struct {
	CommitAfterRetry bool
	HandlerFunc      func(context.Context, *kafka.Message) error
}

type KafkaConsumer struct {
	consumer          *kafka.Consumer
	deserializer      *protobuf.Deserializer
	workerCount       int
	msgQueueLen       int
	msgQueue          chan *kafka.Message
	handlers          map[string]ConsumerHandler
	commitBatchSize   int
	commitInterval    time.Duration
	processedMsgCount int
	handlerAttempts   int
	attemptsDelay     time.Duration
	pausedPartitions  map[string]struct{}
	commitTicker      *time.Ticker
	commitMutex       sync.RWMutex
	pauseMutex        sync.Mutex
	wg                sync.WaitGroup
	shutdown          chan struct{}
}

func NewKafkaConsumer(consumer *kafka.Consumer, workerCount, messageQueueLen int) *KafkaConsumer {
	kc := &KafkaConsumer{
		consumer:         consumer,
		workerCount:      workerCount,
		msgQueueLen:      messageQueueLen,
		msgQueue:         make(chan *kafka.Message, messageQueueLen),
		handlers:         make(map[string]ConsumerHandler),
		shutdown:         make(chan struct{}),
		commitBatchSize:  50,
		commitInterval:   5 * time.Second,
		handlerAttempts:  3,
		attemptsDelay:    100 * time.Millisecond,
		pausedPartitions: make(map[string]struct{}),
	}
	kc.commitTicker = time.NewTicker(kc.commitInterval)

	return kc
}

func (kc *KafkaConsumer) RegisterHandler(topic string, handler ConsumerHandler) {
	kc.handlers[topic] = handler
}

func (kc *KafkaConsumer) Consume() {
	for i := range kc.workerCount {
		kc.wg.Add(1)
		go kc.worker(i)
	}
	// Periodic commit routine
	go kc.commitRoutine()

	kc.consumeLoop()
}

func (kc *KafkaConsumer) consumeLoop() {
	for {
		select {
		case <-kc.shutdown:
			close(kc.msgQueue)
			return
		default:
			msg, err := kc.consumer.ReadMessage(100 * time.Millisecond)
			if err != nil {
				if kafkaErr, ok := err.(kafka.Error); ok && kafkaErr.Code() == kafka.ErrTimedOut {
					continue
				}
				slog.Error("failed to consume message from kafka", "error", err)
				continue
			}

			select {
			case kc.msgQueue <- msg:
			case <-time.After(5 * time.Second):
				slog.Warn("message queue is full", "topic_partition_offset", msg.String(), "queue_size", kc.msgQueueLen)
			}
		}
	}
}

func (kc *KafkaConsumer) worker(id int) {
	defer kc.wg.Done()
	for kafkaMsg := range kc.msgQueue {
		kc.processMessage(id, kafkaMsg)
	}
}

func (kc *KafkaConsumer) processMessage(worker_id int, msg *kafka.Message) {

	// Skip processing if partition is already paused
	key := fmt.Sprintf("%s-%d", *msg.TopicPartition.Topic, msg.TopicPartition.Partition)
	kc.pauseMutex.Lock()
	if _, exists := kc.pausedPartitions[key]; exists {
		kc.pauseMutex.Unlock()
		return
	}
	kc.pauseMutex.Unlock()

	topic := "<unknown>"
	if msg.TopicPartition.Topic != nil {
		topic = *msg.TopicPartition.Topic
	}

	slog.Info("processing message", "topic_partition_offset", msg.String(), "worker_id", worker_id)

	handler, exists := kc.handlers[topic]
	if !exists {
		slog.Info("no handler found for topic", "topic", topic)
		kc.markForCommit(msg)
		return
	}

	resp := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	wasRetried := false
	go func() {
		defer func() {
			if r := recover(); r != nil {
				resp <- fmt.Errorf("consumer handler panic: %v", r)
			}
		}()
		resp <- Retry(
			kc.handlerAttempts,
			kc.attemptsDelay,
			func(n int, err error) {
				slog.Warn(
					"retrying message processing", "topic_partition_offset", msg.String(), "worker_id", worker_id, "attempt", n+1, "error", err,
				)
				wasRetried = true
			},
			func() error {
				return handler.HandlerFunc(ctx, msg)
			},
		)
	}()

	select {
	case err := <-resp:
		if err == nil {
			if wasRetried {
				slog.Info("worker successfully processed message after retries", "topic_partition_offset", msg.String(), "worker_id", worker_id, "recovered", true)
			} else {
				slog.Info("worker successfully processed message", "topic_partition_offset", msg.String(), "worker_id", worker_id)
			}
			kc.markForCommit(msg)

		} else {
			slog.Error("message processing failed after all retries", "topic_partition_offset", msg.String(), "worker_id", worker_id, "errors", err)
			// Handle failures -> send to a topic or database
			if handler.CommitAfterRetry {
				kc.markForCommit(msg)

			} else {

				partitionKey := fmt.Sprintf("%s-%d", *msg.TopicPartition.Topic, msg.TopicPartition.Partition)
				kc.pauseMutex.Lock()
				if _, exists := kc.pausedPartitions[partitionKey]; exists {
					kc.pauseMutex.Unlock()
					return
				}
				err = kc.consumer.Pause([]kafka.TopicPartition{msg.TopicPartition})
				if err != nil {
					slog.Error("failed to pause kafka offset", "topic_partition_offset", msg.String(), "error", err)
				}
				err = kc.consumer.Seek(msg.TopicPartition, 1000)
				if err != nil {
					slog.Error("failed to seek kafka offset", "topic_partition_offset", msg.String(), "error", err)
				}
				kc.pausedPartitions[partitionKey] = struct{}{}
				kc.pauseMutex.Unlock()

				slog.Info("pause and seek kafka", "topic_partition_offset", msg.String())

				go func(tp kafka.TopicPartition, key string) {
					backoff := time.Duration(math.Pow(2, float64(1))) * time.Millisecond // fix
					time.Sleep(backoff)

					kc.pauseMutex.Lock()
					defer kc.pauseMutex.Unlock()

					if _, exists := kc.pausedPartitions[partitionKey]; !exists {
						slog.Info("partition already resumed by another goroutine")
						return
					}
					err = kc.consumer.Resume([]kafka.TopicPartition{tp})
					if err != nil {
						slog.Error("failed to pause resume offset", "topic_partition_offset", msg.String(), "error", err)
					}
					delete(kc.pausedPartitions, partitionKey)

					slog.Info("resume kafka", "topic_partition_offset", msg.String(), "backoff_time", backoff)

				}(msg.TopicPartition, partitionKey)
			}
		}
	case <-ctx.Done():
		slog.Warn("message processing timed out", "topic_partition_offset", msg.String(), "worker_id", worker_id)
		// Handle failures -> send to a topic or database
		if handler.CommitAfterRetry {
			kc.markForCommit(msg)
		}
	}
}

func (kc *KafkaConsumer) markForCommit(msg *kafka.Message) {
	_, err := kc.consumer.StoreMessage(msg)
	if err != nil {
		slog.Error("failed to store message offset", "error", err)
		return
	}

	kc.commitMutex.Lock()
	kc.processedMsgCount++

	// Count-based commit
	if kc.processedMsgCount >= kc.commitBatchSize {
		slog.Info("count-based commit triggered", "bakc._size", kc.commitBatchSize)
		kc.commitMutex.Unlock()
		kc.commitStoredOffsets()

		kc.commitTicker.Reset(kc.commitInterval)
		return
	}
	kc.commitMutex.Unlock()
}

func (kc *KafkaConsumer) commitStoredOffsets() {
	kc.commitMutex.Lock()
	defer kc.commitMutex.Unlock()

	if kc.processedMsgCount == 0 {
		return
	}

	info, err := kc.consumer.Commit()
	if err != nil {
		slog.Error("failed to commit stored offsets", "error", err)
		return
	}

	slog.Info(
		"successfully committed stored offsets", "processed_msg", kc.processedMsgCount, "topic_partition_offset", info[len(info)-1].String(),
	)
	kc.processedMsgCount = 0
}

func (kc *KafkaConsumer) commitRoutine() {
	defer kc.commitTicker.Stop()

	for {
		select {
		case <-kc.shutdown:
			kc.commitStoredOffsets()
			return
		case <-kc.commitTicker.C:
			slog.Info("time-based commit triggered", "interval", kc.commitInterval)
			kc.commitStoredOffsets()
		}
	}
}

func (kc *KafkaConsumer) Stop() {
	close(kc.shutdown)
	kc.wg.Wait()

	kc.commitStoredOffsets()
	if err := kc.consumer.Close(); err != nil {
		slog.Error("closing consumer", "error", err)
	}
}
