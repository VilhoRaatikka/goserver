package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/Shopify/sarama"
)

/*
#include <stdio.h>

typedef void voidStruct;

typedef enum {
	FILEWRITEEVENT=0,
	AUDITEVENT
} monEventType;

typedef struct {
    int   EventID;
    long  ConnID;
	long long  TimeStamp;
	char*  Name;
	char* UserName;
} auditEvent;

typedef struct {
	long Nbytes;
	char* FileName;
	char* FuncName;
} fileWriteEvent;
*/
import "C"

//export Counter
type Counter int64

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

func runServer(laddr net.UnixAddr) {
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

/**
 * Find out the type of input C struct, and copy it to corresponding go struct. This is necessary because json.Marshal-function only can convert go types to json.
 */
func getGoEvent(evType C.monEventType, srcCStruct *C.voidStruct) interface{} {
	switch evType {
	case C.FILEWRITEEVENT:
		return &struct {
			Nbytes   int
			FileName string
			FuncName string
		}{Nbytes: int((*C.fileWriteEvent)(unsafe.Pointer(srcCStruct)).Nbytes),
			FileName: C.GoString((*C.fileWriteEvent)(unsafe.Pointer(srcCStruct)).FileName),
			FuncName: C.GoString((*C.fileWriteEvent)(unsafe.Pointer(srcCStruct)).FuncName)}

	case C.AUDITEVENT:
		return &struct {
			EventID   int
			ConnID    int
			TimeStamp int
			Name      string
			UserName  string
		}{EventID: int((*C.auditEvent)(unsafe.Pointer(srcCStruct)).EventID),
			ConnID:    int((*C.auditEvent)(unsafe.Pointer(srcCStruct)).ConnID),
			TimeStamp: int((*C.auditEvent)(unsafe.Pointer(srcCStruct)).TimeStamp),
			Name:      C.GoString((*C.auditEvent)(unsafe.Pointer(srcCStruct)).Name),
			UserName:  C.GoString((*C.auditEvent)(unsafe.Pointer(srcCStruct)).UserName)}
	}
	return nil
}

func connectMonitorServer(laddr net.UnixAddr) (net.Conn, error) {
	var conn net.Conn
	var err error
	tickChan := time.NewTicker(time.Millisecond * 100)
	// connect monitor server
	conn, err = net.Dial(laddr.Net, laddr.Name)

	if err != nil {
		// Failing Dial indicates that server isn't running
		// Monitor server routine is started and it remains active
		// until process termination or till exit msg is sent
		go runServer(laddr)

		for {
			// re-connect monitor server now when server is supposed to be started and running
			conn, err = net.Dial(laddr.Net, laddr.Name)

			if err != nil {
				select {
				case <-tickChan.C:
					continue
				}
			} else {
				return conn, err
			}
		}
	}
	return conn, err
}

//export addMonStats
func addMonStats(evType C.monEventType,
	ev *C.voidStruct,
	msg string,
	reply *string,
	errReply *string) {

	log.Println("addMonStats")
	var laddr net.UnixAddr
	var conn net.Conn
	var err error
	laddr.Net = "unix"
	laddr.Name = "/tmp/monSocket"
	var goEventIF interface{}

	// Create go struct from c input struct
	goEventIF = getGoEvent(evType, ev)

	if goEventIF == nil {
		*errReply = "Invalid event type structure"
		return
	}

	// Connect monitor server
	conn, err = connectMonitorServer(laddr)
	if err != nil {
		log.Fatalf("Failed to connect monitor server : %s", err)
	}
	// Generate JSON out of input struct
	msgJSON, err := json.Marshal(goEventIF)

	if err != nil {
		log.Fatalf("Failed to create JSON : %s\n", err)
	}
	// Write event type and message as JSON to server
	fmt.Fprintf(conn, "%s %s %s\n", msg, getEventTypeString(evType), string(msgJSON))
	srvReply, err := bufio.NewReader(conn).ReadString('\n')

	if err != nil {
		log.Printf("Client failed to read message : %s", err)
	} else {
		fmt.Println("Server: " + srvReply)
		*reply = srvReply
	}

	defer func() {
		conn.Close()
	}()
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

func main() {
	// We need the main function to make possible
	// CGO compiler to compile the package as C shared library
}

func getEventTypeString(evType C.monEventType) string {
	switch evType {
	case C.FILEWRITEEVENT:
		return "FileWriteEvent"
	case C.AUDITEVENT:
		return "AuditEvent"
	}
	return ""
}
