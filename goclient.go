package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"time"
	"unsafe"

	"github.com/VilhoRaatikka/monitor/goserver"
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
		go goserver.RunServer(laddr)

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
	}
	return ""
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

func main() {
	// We need the main function to make possible
	// CGO compiler to compile the package as C shared library
}
