package main

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func producer() {

	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9093",
	})
	if err != nil {
		panic(err)
	}
	defer producer.Close()

	topic := "bro-conf"
	for i := 0; i < 10; i++ {
		message := fmt.Sprintf("Hello go bro%d", i)
		err := producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny, Offset: kafka.OffsetBeginning},
			Value:          []byte(message),
		}, nil)
		if err != nil {
			fmt.Printf("Failed to produce message: %v\n", err)
		} else {
			fmt.Printf("Produced message: %s\n", message)
		}

		producer.Flush(15 * 1000)
	}
	fmt.Println("All messages produced")
}