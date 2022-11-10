package handlers

import (
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
func KafkaRead(conn *kafka.Conn) (key string, value string) {
	var n kafka.Message
	var err error
	for {
		n, err = conn.ReadMessage(100000)
		if err != nil {
			fmt.Println("err: ", err.Error())
		}
		time.Sleep(time.Millisecond * 100)

		if string(n.Key) != "" {
			break
		}
	}
	return string(n.Key), string(n.Value)
}
