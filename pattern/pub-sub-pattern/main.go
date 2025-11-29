package main

import (
	"time"
)

func main() {
	notificationConsumer1 := NewConsumer("localhost:9093", "notification-group", "yup")
	notificationConsumer2 := NewConsumer("localhost:9093", "notification-group", "yup")
	buisnessConsumer1 := NewConsumer("localhost:9093", "business-group", "yup")
	buisnessConsumer2 := NewConsumer("localhost:9093", "business-group", "yup")

	go notificationConsumer1.Start()
	go notificationConsumer2.Start()
	go buisnessConsumer1.Start()
	go buisnessConsumer2.Start()

	producer := NewProducer("localhost:9093", "yup")
	messages := []string{
		"Order #1001 placed",
		"Order #1002 placed",
		"Order #1003 placed",
		"Order #1004 placed",
		"Order #1005 placed",
	}

	time.Sleep(2 * time.Second) // Give consumers time to start
	producer.SendMessages(messages)

	select {}
}
