package goserver

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"time"

	"sync"

	"encoding/json"

	"github.com/Shopify/sarama"
)

//export Counter
type Counter int64

type EventInfo struct {
	evMap   map[string]Counter
	evMutex *sync.Mutex
}

//export goserver.runServer
func RunServer(laddr net.UnixAddr) {
	log.Println("> runServer")
	var conn net.Conn
	var err error
	eventInfo := EventInfo{evMap: map[string]Counter{}, evMutex: &sync.Mutex{}}
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
		go monRequestServe(conn, doneChan, &eventInfo)
	}
}

func listenUnixSocket(laddr net.UnixAddr) *net.UnixListener {
	log.Println("> listenUnixSocket")
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
	evInfo *EventInfo) {

	var messages []string
	log.Println("> monRequestServe")

	for {
		// Listen for msgJSONs ending in newline (\n)
		msgJSON, err := bufio.NewReader(conn).ReadString('\n')

		if err != nil {
			log.Printf("Reading client message failed : %s", err)
			break
		} else {
			log.Printf("Monitor server received from client : %s\n", msgJSON)
			messages = strings.SplitN(msgJSON, " ", 3)

			if strings.EqualFold(messages[0], "add") {
				addEvent(conn, messages[1], evInfo)
				// Send JSON to kafka
				err = produceSyncMessage(messages[1:])
				if err != nil {
					log.Printf("Producing sync message failed : %s", err)
				}
			} else if strings.EqualFold(messages[0], "get") {
				getStats(conn, evInfo)
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

	log.Println("> produceSyncMessage")
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
		Value: sarama.StringEncoder(strings.TrimSuffix(msgstr[1], "\n"))}

	partition, offset, err := producer.SendMessage(msg)

	if err != nil {
		errReply = fmt.Sprintf("FAILED to send message: %s\n", err)
		log.Printf(errReply)
	} else {
		log.Printf("sent %s:%s to partition %d at offset %d\n", msgstr[0], sarama.StringEncoder(strings.TrimSuffix(msgstr[1], "\n")), partition, offset)
	}
	return err
}

func addEvent(conn net.Conn, category string, evInfo *EventInfo) {
	log.Println("> addEvent")
	// Lock
	(*evInfo).evMutex.Lock()
	_, ok := (*evInfo).evMap[category]
	if !ok {
		(*evInfo).evMap[category] = 1
	} else {
		(*evInfo).evMap[category]++
	}
	log.Println((*evInfo).evMap)
	// Unlock
	(*evInfo).evMutex.Unlock()
	s, _ := json.Marshal((*evInfo).evMap)
	conn.Write([]byte(fmt.Sprintln(string(s))))
}

func getStats(conn net.Conn, evInfo *EventInfo) {
	log.Println("> getStats")
	// Lock
	(*evInfo).evMutex.Lock()
	s, _ := json.Marshal((*evInfo).evMap)
	// Unlock
	(*evInfo).evMutex.Unlock()
	conn.Write([]byte(fmt.Sprintln(string(s))))
}

func sendUsage(conn net.Conn) {
	log.Println("> sendUsage")
	conn.Write([]byte("Invalid input. Accepted inputs are : " +
		"\"add\", \"get\", or \"quitmonitor\".\n"))
}

func sendExit(conn net.Conn) {
	log.Println("> sendExit")
	conn.Write([]byte("quit\n"))
}
