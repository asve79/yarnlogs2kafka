package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/Shopify/sarama"
)

func main() {
	topic := flag.String("topic", "test-file-send", "Topic")
	brockerlist := []string{"localhost:9092"}
	deleteAfterSend := flag.Bool("deleteAfterSend", false, "Delete file after send")

	scanpath := flag.String("scanPath", "./", "Scan directory")

	flag.Parse()

	producer, err := newProducer(brockerlist)
	if err != nil {
		log.Fatal("error init producer: ", err.Error())
	}

	list, err := FilePathWalkDir(*scanpath)
	if err != nil {
		for _, file := range list {
			readFile(file, producer, *topic, *deleteAfterSend)
		}
	}
}

func FilePathWalkDir(root string) ([]string, error) {
	var files []string
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			files = append(files, path)
		}
		return nil
	})
	return files, err
}

func readFile(filename string, producer sarama.SyncProducer, topic string, deleteAfterSend bool) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		fmt.Println("Sending: " + line)
		msg := prepareMessage(topic, line)

		_, _, err := producer.SendMessage(msg)
		if err != nil {
			panic(err)
		}

	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	if deleteAfterSend {
		err = os.Remove(filename)
		if err != nil {
			log.Fatal("error file removing", err.Error())
		}
	}
}

func newProducer(brokers []string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer(brokers, config)

	return producer, err
}

func prepareMessage(topic, message string) *sarama.ProducerMessage {
	msg := &sarama.ProducerMessage{
		Topic:     topic,
		Partition: -1,
		Value:     sarama.StringEncoder(message),
	}

	return msg
}
