#include <stdio.h>
#include <unistd.h>
#include "goclient.h"
#include <string.h>
#include <pthread.h>
#include <sys/time.h>

int main()
{
	auditEvent a, b, c;
	fileWriteEvent f;
	const char* fw_name = "File name";
	const char* fw_funcname = "Function name";
	struct timeval tv;
	GoString reply_msg;
	GoString topic = {"cramon_test",strlen("cramon_test")};
	GoString err_reply;
	GoString addmsg = {"add", strlen("add")};
	GoString getmsg = {"get", strlen("get")};
	GoString quitmsg = {"quitmonitor", strlen("quitmonitor")};
	char s[256];
	reply_msg.p = s;
	reply_msg.n = 255;
	monEventType type;

	gettimeofday(&tv, NULL);
	a.ConnID=1;
	a.EventID=123;
	a.TimeStamp=tv.tv_sec;
	a.Name="Some name";
	a.UserName="Test User 1";

	f.Nbytes = 123321;
	f.FileName = strndup(fw_name, strlen(fw_name)+1);
	f.FuncName = strndup(fw_funcname, strlen(fw_funcname));

	printf("Before getMonStats\n");
	addMonStats(AUDITEVENT, &a, addmsg, &reply_msg, &err_reply);
	printf("After 1st getMonStats, counter : %s\n", reply_msg.p);
	addMonStats(AUDITEVENT, &a, getmsg, &reply_msg, &err_reply);
	printf("After 2nd getMonStats, counter : %s\n", reply_msg.p);
	addMonStats(AUDITEVENT, &a, addmsg, &reply_msg, &err_reply);
	printf("After 3rd getMonStats, counter : %s\n", reply_msg.p);
	addMonStats(AUDITEVENT, &a, quitmsg, &reply_msg, &err_reply);
	printf("After 4th getMonStats, counter : %s\n", reply_msg.p);

	addMonStats(FILEWRITEEVENT, &f, addmsg, &reply_msg, &err_reply);
	printf("After 5th getMonStats, counter : %s\n", reply_msg.p);
	sleep(10);
	return 1;
}
