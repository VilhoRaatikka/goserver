# goserver
Build static library to be linked with c executable:

go build -buildmode=c-archive -o goclient.a goclient.go

Build c test program:

gcc -o cgo cgo.c ./goclient.a  -pthread
