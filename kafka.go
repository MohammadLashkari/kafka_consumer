package main

import (
	"os"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/protobuf"
)

func bootKafkaConsumer() (*kafka.Consumer, error) {
	consumerCfg := kafka.ConfigMap{
		"bootstrap.servers":        os.Getenv("KAFKA_BOOTSTRAP_SERVER"),
		"group.id":                 os.Getenv("KAFKA_GROUP_ID"),
		"auto.offset.reset":        "earliest",
		"enable.auto.commit":       false,
		"enable.auto.offset.store": false,
	}
	consumer, err := kafka.NewConsumer(&consumerCfg)
	if err != nil {
		return nil, err
	}

	topics := []string{os.Getenv("KAFKA_TOPIC")}
	err = consumer.SubscribeTopics(topics, nil)
	if err != nil {
		return nil, err
	}

	return consumer, nil
}

func bootSchemaRegistry() (*protobuf.Deserializer, error) {
	schemaCfg := schemaregistry.NewConfig("")
	schemaCfg.BasicAuthCredentialsSource = "URL"
	schemaClient, err := schemaregistry.NewClient(schemaCfg)
	if err != nil {
		return nil, err
	}
	deserializer, err := protobuf.NewDeserializer(schemaClient, serde.ValueSerde, protobuf.NewDeserializerConfig())
	if err != nil {
		return nil, err
	}

	return deserializer, nil
}
