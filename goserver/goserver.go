package goserver

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Shopify/sarama"
)

//export Counter
type Counter int64

//export goserver.runServer
func RunServer(laddr net.UnixAddr) {
	log.Println("runServer")
	var conn net.Conn
	var err error
	var counter Counter
	doneChan := make(chan bool)

	ln := listenUnixSocket(laddr)

	defer ln.Close()

	for {
		select {
		case <-doneChan:
			return

		default:
			fmt.Println("Accept")
			// accept connection on port
			ln.SetDeadline(time.Now().Add(1 * time.Second))
			conn, err = ln.Accept()

			if err != nil {
				log.Printf("Accept failed : %s\n", err)
				continue
			}
		}
		// Launch server routine for reading and processing data
		// from the connection
		go monRequestServe(conn, doneChan, &counter)
	}
}

func listenUnixSocket(laddr net.UnixAddr) *net.UnixListener {
	log.Println("listenUnixSocket")
	var ln *net.UnixListener
	// listen client connections on socket
	ln, err := net.ListenUnix(laddr.Net, &laddr)

	for err != nil {
		log.Printf("Listen failed : %s\n", err)
		os.Remove(laddr.Name)
		ln, err = net.ListenUnix(laddr.Net, &laddr)
	}
	return ln
}

func monRequestServe(conn net.Conn,
	doneChan chan<- bool,
	counter *Counter) {

	var messages []string
	log.Println("monRequestServe")

	for {
		// Listen for msgJSONs ending in newline (\n)
		msgJSON, err := bufio.NewReader(conn).ReadString('\n')

		if err != nil {
			log.Printf("Reading client message failed : %s", err)
			break
		} else {
			log.Printf("Monitor server received from client : %s", msgJSON)
			messages = strings.SplitN(msgJSON, " ", 3)

			if strings.EqualFold(messages[0], "add") {
				addEvent(conn, msgJSON, counter)
				// Send JSON to kafka
				err = produceSyncMessage(messages[1:])
				if err != nil {
					log.Printf("Producing sync message failed : %s", err)
				}
			} else if strings.EqualFold(messages[0], "get") {
				getStats(conn, msgJSON, counter)
			} else if strings.EqualFold(messages[0], "quitmonitor") {
				sendExit(conn)
				doneChan <- true
				break
			} else {
				sendUsage(conn)
			}
		}
	}
}

// Produce a messeage to kafka
func produceSyncMessage(msgstr []string) error {
	var errReply string

	log.Println("produceSyncMessage")
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5

	brokers := []string{"kafkabroker1:9092", "kafkabroker2:9092", "kafkabroker3:9092"}
	producer, err := sarama.NewSyncProducer(brokers, config)

	if err != nil {
		errReply = fmt.Sprintf("FAILED to send message: %s\n", err)
		log.Printf(errReply)
		return err
	}

	defer func() {
		if err := producer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()
	// Create Kafka message consisting of topic and value, from string array
	msg := &sarama.ProducerMessage{
		Topic: msgstr[0],
		Value: sarama.StringEncoder(msgstr[1])}

	partition, offset, err := producer.SendMessage(msg)

	if err != nil {
		errReply = fmt.Sprintf("FAILED to send message: %s\n", err)
		log.Printf(errReply)
		return err
	} else {
		log.Printf("> message sent to partition %d at offset %d\n", partition, offset)
	}
	return nil
}

func addEvent(conn net.Conn, msg string, counter *Counter) {
	log.Println("addEvent")
	atomic.AddInt64((*int64)(counter), 1)
	conn.Write([]byte(fmt.Sprintf("%d\n", *counter)))
}

func getStats(conn net.Conn, msg string, counter *Counter) {
	log.Println("getStats")
	conn.Write([]byte(fmt.Sprintf("%d\n", *counter)))
}

func sendUsage(conn net.Conn) {
	log.Println("sendUsage")
	conn.Write([]byte("Invalid input. Accepted inputs are : " +
		"\"add\", \"get\", or \"quitmonitor\".\n"))
}

func sendExit(conn net.Conn) {
	log.Println("sendExit")
	conn.Write([]byte("quit\n"))
}
