package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/linkedin/goavro/v2"
	"io/ioutil"
	"kafka-producer/dto"
)

var (
	codec *goavro.Codec
)

func init() {

	schema, err := ioutil.ReadFile("dto/book-schema.avsc")
	if err != nil {
		panic(err)
	}

	codec, err = goavro.NewCodec(string(schema))
	if err != nil {
		panic(err)
	}

}

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
	topic := "bookTopic"

	book1 := dto.Book{
		ID: "Book_1_Unique_ID",
		Title: "Stranger In A Strange Land",
		Author: dto.Author{
			FirstName: "Robert",
			LastName: "Heinlen",
		},
	}

	book2 := dto.Book{
		ID: "Book_2_Unique_ID",
		Title: "Hitchhiker's Guide to the Galaxy",
		Author: dto.Author{
			FirstName: "Douglas",
			LastName: "Adams",
		},
	}

	books := make([]dto.Book, 2)
	books[0] = book1
	books[1] = book2

	for _, book := range books {

		binaryBook, err := codec.BinaryFromNative(nil, book.ToStringMap())
		if err != nil {
			panic(err)
		}

		producer.Produce(
			&kafka.Message{
				TopicPartition: kafka.TopicPartition{
					Topic: &topic,
					Partition: kafka.PartitionAny,
				},
				Value: binaryBook,
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
