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

	"io"

	"github.com/Shopify/sarama"
)

//export Counter
type Counter int64

type EventInfo struct {
	evMap   map[string]Counter
	evMutex *sync.Mutex
}

//export goserver.RunServer
// Server thread. Runs until process termination or until exit message
func RunServer(laddr net.UnixAddr) {
	// log.Println("> runServer")
	var conn net.Conn
	var err error
	var kafkaSyncProducer sarama.SyncProducer
	var kafkaBrokers []string
	eventInfo := EventInfo{evMap: map[string]Counter{}, evMutex: &sync.Mutex{}}
	doneChan := make(chan bool)

	ln := listenUnixSocket(laddr)

	defer ln.Close()
	// Loop every second and accept connections until message appears on
	// doneChan
	for {
		select {
		case <-doneChan:
			return

		default:
			// fmt.Println("Before Accept")
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
		go monRequestServe(conn,
			kafkaSyncProducer,
			kafkaBrokers,
			doneChan,
			&eventInfo)
	}
}

func listenUnixSocket(laddr net.UnixAddr) *net.UnixListener {
	// log.Println("> listenUnixSocket")
	var ln *net.UnixListener
	// listen client connections on socket
	ln, err := net.ListenUnix(laddr.Net, &laddr)

	for err != nil {
		// log.Printf("Listen failed : %s\n", err)
		os.Remove(laddr.Name)
		ln, err = net.ListenUnix(laddr.Net, &laddr)
	}
	return ln
}

func monRequestServe(conn net.Conn,
	kafkaSyncProducer sarama.SyncProducer,
	kafkaBrokers []string,
	doneChan chan<- bool,
	evInfo *EventInfo) {

	var messages []string
	// log.Println("> monRequestServe")

	for {
		// Listen for msgJSONs ending in newline (\n)
		msgJSON, err := bufio.NewReader(conn).ReadString('\n')

		if err != nil {
			if err != io.EOF {
				log.Printf("Reading client message failed : %s", err)
			}
			break
		} else {
			// log.Printf("Received from client : %s\n", msgJSON)
			messages = strings.SplitN(msgJSON, " ", 4)

			if strings.EqualFold(messages[0], "add") {
				// Send all but 1st string to mesage service
				kafkaSyncProducer, kafkaBrokers, err = produceSyncMessage(kafkaSyncProducer,
					kafkaBrokers,
					messages[1:])
				if err != nil {
					log.Printf("Producing sync message failed : %s", err)
				}
				// Write back to client
				addEvent(conn, messages[1], evInfo)
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

// Produce a messeage to messaging server
func produceSyncMessage(kafkaSyncProducer sarama.SyncProducer,
	kafkaBrokers []string,
	msgstr []string) (sarama.SyncProducer, []string, error) {

	var errReply string
	var err error

	// log.Println("> produceSyncMessage")
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Errors = true
	config.Producer.Return.Successes = true
	config.Producer.Retry.Max = 5
	err = config.Validate()

	if err != nil {
		errReply = fmt.Sprintf("Configuration is not valid: %s\n", err)
		log.Printf(errReply)
		return nil, kafkaBrokers, err
	}

	// Create producer if not creted yet
	if kafkaSyncProducer == nil {
		kafkaBrokers = []string{"kafkabroker1:9092", "kafkabroker2:9092", "kafkabroker3:9092"}
		kafkaSyncProducer, err = sarama.NewSyncProducer(kafkaBrokers, config)

		if err != nil {
			errReply = fmt.Sprintf("Failed to create producer: %s\n", err)
			log.Printf(errReply)
			return nil, kafkaBrokers, err
		}
	}
	// Create Kafka message consisting of topic and value, from string array
	msg := &sarama.ProducerMessage{
		Topic: msgstr[0],
		Key:   sarama.StringEncoder(strings.TrimSuffix(msgstr[1], "\n")),
		Value: sarama.StringEncoder(strings.TrimSuffix(msgstr[2], "\n"))}

	partition, offset, err := kafkaSyncProducer.SendMessage(msg)

	if err != nil {
		errReply = fmt.Sprintf("Failed to send message: %s\n", err)
		log.Printf(errReply)
	} else {
		log.Printf("sent %s %s %s to partition %d at offset %d\n",
			msgstr[0],
			sarama.StringEncoder(strings.TrimSuffix(msgstr[1], "\n")),
			sarama.StringEncoder(strings.TrimSuffix(msgstr[2], "\n")),
			partition,
			offset)
	}
	return kafkaSyncProducer, kafkaBrokers, err
}

func addEvent(conn net.Conn, category string, evInfo *EventInfo) {
	// log.Println("> addEvent")
	// Lock
	(*evInfo).evMutex.Lock()
	_, ok := (*evInfo).evMap[category]
	if !ok {
		(*evInfo).evMap[category] = 1
	} else {
		(*evInfo).evMap[category]++
	}
	s, err := json.Marshal((*evInfo).evMap)
	// Unlock
	(*evInfo).evMutex.Unlock()

	if err != nil {
		conn.Write([]byte(fmt.Sprintf("Failed to create JSON: %s\n", err)))
	} else {
		conn.Write([]byte(fmt.Sprintln(string(s))))
	}
}

func getStats(conn net.Conn, evInfo *EventInfo) {
	// log.Println("> getStats")
	// Lock
	(*evInfo).evMutex.Lock()
	s, err := json.Marshal((*evInfo).evMap)
	// Unlock
	(*evInfo).evMutex.Unlock()

	if err != nil {
		conn.Write([]byte(fmt.Sprintf("Failed to create JSON: %s\n", err)))
	} else {
		conn.Write([]byte(fmt.Sprintln(string(s))))
	}
}

func sendUsage(conn net.Conn) {
	// log.Println("> sendUsage")
	conn.Write([]byte("Invalid input. Accepted inputs are : " +
		"\"add\", \"get\", or \"quitmonitor\".\n"))
}

func sendExit(conn net.Conn) {
	// log.Println("> sendExit")
	conn.Write([]byte("quit\n"))
}
