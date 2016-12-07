# goserver
Build static library to be linked with c executable:

go build -buildmode=c-archive -o goserver.a goserver.go

Build c test program:

gcc -o cgo cgo.c ./goserver.a  -pthread
