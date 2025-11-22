package main

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func producer(msgArgs []string) {

	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9093",
	})
	if err != nil {
		panic(err)
	}
	defer producer.Close()

	topic := "bro-conf"
	for i := 0; i < len(msgArgs); i++ {
		message := msgArgs[i]
		key := fmt.Sprintf("key-%d", i%3)
		err := producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny, Offset: kafka.OffsetBeginning},
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