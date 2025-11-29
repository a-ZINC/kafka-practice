package main

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Producer struct {
	bootstrapServers string
	topic            string
}

func NewProducer(bootstrapServers, topic string) *Producer {
	return &Producer{
		bootstrapServers: bootstrapServers,
		topic:            topic,
	}
}
func (p *Producer) SendMessages(msgArgs []string) {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": p.bootstrapServers,
	})
	if err != nil {
		panic(err)
	}
	defer producer.Close()

	for i := 0; i < len(msgArgs); i++ {
		message := msgArgs[i]
		key := fmt.Sprintf("key-%d", i%3)
		err := producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &p.topic, Partition: kafka.PartitionAny, Offset: kafka.OffsetBeginning},
			Value:          []byte(message),
			Key:            []byte(key),
		}, nil)
		if err != nil {
			fmt.Printf("Failed to produce message: %v\n", err)
		} else {
			fmt.Printf("Produced message: %s\n", message)
		}

		producer.Flush(5 * 1000)
	}
	fmt.Println("All messages produced")
}
