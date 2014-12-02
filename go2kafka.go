package main

import (
	"bufio"
	"flag"
	"fmt"
	. "github.com/Shopify/sarama"
	"log"
	"os/exec"
	"time"
)

func wiretapLog(filename string) (outputCh chan string) {
	outputCh = make(chan string)

	go func(dataCh chan<- string) {
		tailCmd := exec.Command("tail", "-n", "0", "-F", filename)
		tailOut, err := tailCmd.StdoutPipe()
		if err != nil {
			log.Fatalln("[Error] Failed to capture stdout handle of tail cmd")
			panic(err)
		}
		tailCmd.Start()

		lineScanner := bufio.NewScanner(tailOut)
		var ok bool
		for {
			ok = lineScanner.Scan()

			if ok != true {
				if err := lineScanner.Err(); err != nil {
					log.Fatalln("[Error] Failed to capture line of tail cmd")
					panic(err)
				}
				log.Println("Done tailing (EOF). Good bye.")
				close(dataCh)
				break
			}

			dataCh <- lineScanner.Text()
		}
	}(outputCh)

	return outputCh
}

func createKafkaClient(addr string) *Client {

	kAddrs := append([]string{}, addr)
	log.Println("kAddrs:", kAddrs)
	kClientId := fmt.Sprintf("go2kafka_%d", time.Now().Unix())
	kClient, err := NewClient(kClientId, kAddrs, NewClientConfig())
	if err != nil {
		log.Println("[Error] Failed to connect to kafka")
		panic(err)
	}

	return kClient
}

func createKafkaProducer(client *Client, topic string) *SimpleProducer {

	prod, err := NewSimpleProducer(client, topic, nil)
	if err != nil {
		log.Println("[Error] Failed to create kafka producer")
		panic(err)
	}

	return prod
}

func main() {
	var logFilename, kAddr, kTopic string
	flag.StringVar(&logFilename, "file", "", "file to tail")
	flag.StringVar(&kAddr, "kaddr", "192.168.59.103:49159", "kafka server host:port")
	flag.StringVar(&kTopic, "ktopic", "go2kafka", "kafka topic")
	flag.Parse()

	if logFilename == "" {
		log.Fatalln("[Error] Need a filename to tail")
	}
	if kAddr == "" {
		log.Fatalln("[Error] Kafka host:port required")
	}
	if kTopic == "" {
		log.Fatalln("[Error] Kafka topic required")
	}

	kClient := createKafkaClient(kAddr)
	defer kClient.Close()

	kProd := createKafkaProducer(kClient, kTopic)
	defer kProd.Close()

	wiretapCh := wiretapLog(logFilename)

	// Begin sending logs to kafka...
	for line := range wiretapCh {
		err := kProd.SendMessage(nil, StringEncoder(line))
		if err != nil {
			log.Println("[Error] Failed to send message to kafka")
			panic(err)
		}
		log.Println("line:", line)
	}

}
