package kafkahandlers

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

func KafkaConsumer() *kafka.Reader {
	fmt.Println("msg from Consumer")

	// to consume messages
	topic := "my-topic-1"
	partition := 0
	var conn *kafka.Reader
	brokerAddress := []string{"kafka_0:9092", "kafka_1:9093", "kafka_2:9094"}
	conn = kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokerAddress,
		Topic:   topic,
		// GroupID: "group-rating",
		Partition: partition,
	})
	return conn

}
func KafkaRead(conn *kafka.Reader, ctx context.Context) (key string, value string) {
	var n kafka.Message
	var err error
	topic := "my-topic-1"
	partition := 0
	// var conn *kafka.Reader

	brokerAddress := []string{"kafka_0:9092", "kafka_1:9093", "kafka_2:9094"}
	conn = kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokerAddress,
		Topic:   topic,
		// GroupID: "group-rating",
		Partition: partition,
	})
	time.Sleep(time.Second * 1)
	fmt.Println("reading")
	for {
		n, err = conn.ReadMessage(ctx)
		if err != nil {
			fmt.Println("err: ", err.Error())
		}
		time.Sleep(time.Millisecond * 100)

		if string(n.Key) != "" {
			fmt.Println("read")
			break
		}
	}
	return string(n.Key), string(n.Value)
}
