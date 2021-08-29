package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {

	producer, err := kafka.NewProducer(
		&kafka.ConfigMap{
			"bootstrap.servers": "localhost",
		},
	)
	if err != nil {
		panic(err)
	}
	defer producer.Close()

	responseChan := make(chan kafka.Event, 10000)
	defer close(responseChan)

	// produce messages to topic (async)
	topic := "myTopic"
	for _, word := range []string{"Welcome", "to", "the", "Confluent", "Kafka", "Golang", "client"} {
		producer.Produce(
			&kafka.Message{
				TopicPartition: kafka.TopicPartition{
					Topic: &topic,
					Partition: kafka.PartitionAny,
				},
				Value: []byte(word),
			},
			responseChan,
		)
	}

	go func(responseChannel chan kafka.Event) {
		for {
			event :=  <-responseChannel
			switch ev := event.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
				}
			}
		}
	}(responseChan)

	producer.Flush(15 * 1000)
}
