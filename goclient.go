package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"time"
	"unsafe"

	"github.com/VilhoRaatikka/goserver/goserver"
)

/*
#include <stdio.h>

typedef void voidStruct;

typedef enum {
	FILEWRITEEVENT=0,
	AUDITEVENT,
	DBWRITEEVENT
} monEventType;

typedef struct {
    long  ConnID;
    int   EventID;
	long long  TimeStamp;
	char*  Name;
	char* UserName;
} auditEvent;

typedef struct {
	long ConnID;
	long Nbytes;
	char* FileName;
	char* FuncName;
} fileWriteEvent;

typedef struct {
	long ConnID;
	long long TimeStamp;
	char* DBName;
	char* TableName;
	char* UserName;
} dbWriteEvent;
*/
import "C"

/**
 * Find out the type of input C struct, and copy it to corresponding go struct. This is necessary because json.Marshal-function only can convert go types to json.
 */
func getGoEvent(evType C.monEventType, srcCStruct *C.voidStruct) (int, interface{}) {
	switch evType {
	case C.FILEWRITEEVENT:
		return int((*C.fileWriteEvent)(unsafe.Pointer(srcCStruct)).ConnID), &struct {
			ConnID   int
			Nbytes   int
			FileName string
			FuncName string
		}{ConnID: int((*C.fileWriteEvent)(unsafe.Pointer(srcCStruct)).ConnID),
			Nbytes:   int((*C.fileWriteEvent)(unsafe.Pointer(srcCStruct)).Nbytes),
			FileName: C.GoString((*C.fileWriteEvent)(unsafe.Pointer(srcCStruct)).FileName),
			FuncName: C.GoString((*C.fileWriteEvent)(unsafe.Pointer(srcCStruct)).FuncName)}

	case C.AUDITEVENT:
		return int((*C.auditEvent)(unsafe.Pointer(srcCStruct)).ConnID), &struct {
			ConnID    int
			EventID   int
			TimeStamp int
			Name      string
			UserName  string
		}{ConnID: int((*C.auditEvent)(unsafe.Pointer(srcCStruct)).ConnID),
			EventID:   int((*C.auditEvent)(unsafe.Pointer(srcCStruct)).EventID),
			TimeStamp: int((*C.auditEvent)(unsafe.Pointer(srcCStruct)).TimeStamp),
			Name:      C.GoString((*C.auditEvent)(unsafe.Pointer(srcCStruct)).Name),
			UserName:  C.GoString((*C.auditEvent)(unsafe.Pointer(srcCStruct)).UserName)}

	case C.DBWRITEEVENT:
		return int((*C.dbWriteEvent)(unsafe.Pointer(srcCStruct)).ConnID), &struct {
			ConnID    int
			TimeStamp int
			DBName    string
			TableName string
			UserName  string
		}{ConnID: int((*C.dbWriteEvent)(unsafe.Pointer(srcCStruct)).ConnID),
			TimeStamp: int((*C.dbWriteEvent)(unsafe.Pointer(srcCStruct)).TimeStamp),
			DBName:    C.GoString((*C.dbWriteEvent)(unsafe.Pointer(srcCStruct)).DBName),
			TableName: C.GoString((*C.dbWriteEvent)(unsafe.Pointer(srcCStruct)).TableName),
			UserName:  C.GoString((*C.dbWriteEvent)(unsafe.Pointer(srcCStruct)).UserName)}
	}
	return 0, nil
}

func connectMonitorServer(servedChan chan<- bool, laddr net.UnixAddr) (net.Conn, error) {
	var conn net.Conn
	var err error
	tickChan := time.NewTicker(time.Millisecond * 100)
	// connect monitor server
	conn, err = net.Dial(laddr.Net, laddr.Name)

	if err != nil {
		// Server isn't running, so start and keep it running
		// until process termination or till exit msg is sent
		go goserver.RunServer(servedChan, laddr)

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

func getEventTypeString(evType C.monEventType) string {
	switch evType {
	case C.FILEWRITEEVENT:
		return "FileWriteEvent"
	case C.AUDITEVENT:
		return "AuditEvent"
	case C.DBWRITEEVENT:
		return "DBWriteEvent"
	}
	return ""
}

//export addMonStats
func addMonStats(evType C.monEventType,
	ev *C.voidStruct,
	msg string,
	reply *string,
	errReply *string) {

	// log.Println("addMonStats")
	var laddr net.UnixAddr
	var conn net.Conn
	var err error
	var goEventIF interface{}
	var connID int
	var servedChan chan bool
	laddr.Net = "unix"
	laddr.Name = "/tmp/monSocket"

	// Create go struct from c input struct
	connID, goEventIF = getGoEvent(evType, ev)

	if goEventIF == nil {
		*errReply = "Invalid event type structure"
		return
	}
	// Create notifier channel and connect the server routine
	servedChan = make(chan bool, 1)
	conn, err = connectMonitorServer(servedChan, laddr)

	if err != nil {
		log.Fatalf("Failed to connect monitor server : %s", err)
	}
	// Generate JSON out of input struct
	msgJSON, err := json.Marshal(goEventIF)

	if err != nil {
		log.Fatalf("Failed to create JSON : %s\n", err)
	}
	// Write event type and message as JSON to server
	var n int
	n, err = fmt.Fprintf(conn,
		"%s %s %d %s\n",
		msg, getEventTypeString(evType),
		connID,
		string(msgJSON))

	if err != nil {
		log.Printf("Writing to server failed : %s", err)
	}
	srvReply, err := bufio.NewReader(conn).ReadString('\n')

	if err != nil {
		log.Printf("Client failed to read message : %s", err)
	} else {
		// fmt.Println("From server: " + srvReply)
		*reply = srvReply
	}
	// Wait until server has completed writing
	log.Println("Entering select")
	select {
	case <-servedChan:
		log.Printf("Wrote %d bytes to server\n", n)
	}
	log.Println("Exiting select")

	defer func() {
		conn.Close()
	}()
}

func main() {
	// We need the main function to make possible
	// CGO compiler to compile the package as C shared library
}
