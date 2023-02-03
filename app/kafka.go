package app

import (
	kingpin "gopkg.in/alecthomas/kingpin.v2"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync"

	"github.com/Shopify/sarama"
)

var (
	brokerList        = kingpin.Flag("brokerList", "List of brokers to connect").Default("localhost:9092", "localhost:9093", "localhost:9094").Strings()
	topic             = kingpin.Flag("topic", "Topic name").Default("counter13").String()
	partition         = kingpin.Flag("partition", "Partition number").Default("0").String()
	offsetType        = kingpin.Flag("offsetType", "Offset Type (OffsetNewest | OffsetOldest)").Default("-1").Int()
	messageCountStart = kingpin.Flag("messageCountStart", "Message counter start from:").Int()
)

var Total int

var mutex sync.Mutex

func Consume() {
	kingpin.Parse()

	config := sarama.NewConfig()

	config.Consumer.Return.Errors = true

	brokers := *brokerList

	master, err := sarama.NewConsumer(brokers, config)

	if err != nil {
		log.Panic(err)
	}

	defer func() {
		if err := master.Close(); err != nil {
			log.Panic(err)
		}
	}()

	consumer, err := master.ConsumePartition(*topic, 0, sarama.OffsetOldest)

	if err != nil {
		log.Panic(err)
	}

	signals := make(chan os.Signal, 1)

	signal.Notify(signals, os.Interrupt)

	doneCh := make(chan struct{})

	group := &sync.WaitGroup{}

	go streamData(consumer, group, signals, doneCh)

	group.Wait()

	<-doneCh

	log.Println("Processed", *messageCountStart, "messages")

	log.Println("TOTAL :", Total)
}

func streamData(consumer sarama.PartitionConsumer, group *sync.WaitGroup, signals chan os.Signal, doneCh chan struct{}) {
	for {
		select {

		case err := <-consumer.Errors():
			log.Println(err)
		case msg := <-consumer.Messages():
			*messageCountStart++

			valFromKafka, _ := strconv.Atoi(string(msg.Value))

			go func(i int) {
				defer group.Done()

				group.Add(1)

				mutex.Lock()
				log.Println("Current ", Total, "From Kafka ", i)
				Total = Total + valFromKafka
				mutex.Unlock()
			}(valFromKafka)

			//log.Println("Received messages key :", string(msg.Key), " value :", string(msg.Value))

			//log.Println("Total = " + total)
		case <-signals:

			log.Println("Interrupt is detected")

			doneCh <- struct{}{}
		}

	}

}
