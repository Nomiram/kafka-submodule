package kafkahandlers

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

func KafkaProducer() *kafka.Writer {
	fmt.Println("msg from Producer")
	// to produce messages
	topic := "my-topic-1"
	w := &kafka.Writer{
		Addr: kafka.TCP("kafka_0:9092", "kafka_1:9093", "kafka_2:9094"),
		// Addr:     kafka.TCP("localhost:9092", "localhost:9093", "localhost:9094"),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}
	return w
}
func KafkaWrite(w *kafka.Writer, key string, value string) {
	for {

		err := w.WriteMessages(context.Background(),
			kafka.Message{
				Key:   []byte(key),
				Value: []byte(value),
			},
		)
		if err != nil {
			fmt.Println("failed to write messages:", err)
			time.Sleep(time.Second * 2)
		} else {
			break
		}

	}
}
