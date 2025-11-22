package main

import (
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func consumer() {
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9093",
		"group.id": "bro-conf-group",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		panic(err)
	}
	defer func() {
		err := consumer.Close()
		if err != nil {
			fmt.Printf("Failed to close consumer: %v\n", err)
		}
	}()

	topic := "bro-conf"
	err = consumer.Subscribe(topic, nil)
	if err != nil {
		panic(err)
	}

	for {
		msg, err := consumer.ReadMessage(5 * time.Second)
		if err != nil {
			// Handle timeout or other errors
			kafkaErr, ok := err.(kafka.Error)
			if ok && kafkaErr.Code() == kafka.ErrTimedOut {
				fmt.Println("No new messages, waiting...")
				continue
			}
			fmt.Printf("Failed to consume message: %v\n", err)
			continue
		}
		time.Sleep(5 * time.Second)
		fmt.Printf("Consumed message: %s\n", string(msg.Value))
	}
}