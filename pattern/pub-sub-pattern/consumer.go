package main

import (
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Consumer struct {
	bootstrapServers string
	groupID          string
	topic            string
}

func NewConsumer(bootstrapServers, groupID, topic string) *Consumer {
	return &Consumer{
		bootstrapServers: bootstrapServers,
		groupID:          groupID,
		topic:            topic,
	}
}

func (c *Consumer) Start() {
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": c.bootstrapServers,
		"group.id":          c.groupID,
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

	err = consumer.Subscribe(c.topic, nil)
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
		time.Sleep(1 * time.Second)
		fmt.Printf("Consumed message: %s from %s %s\n", string(msg.Value), c.groupID, consumer.String())
	}
}