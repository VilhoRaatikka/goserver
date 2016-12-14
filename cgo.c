#include <stdio.h>
#include <unistd.h>
#include "goclient.h"
#include <string.h>
#include <pthread.h>
#include <sys/time.h>
#include <syslog.h>

#define NMSGS 5000

int main()
{
	auditEvent a, b, c;
	fileWriteEvent f;
	dbWriteEvent d;
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
	int i;
	reply_msg.p = s;
	reply_msg.n = 255;
	monEventType type;

	gettimeofday(&tv, NULL);
	a.EventID=123;
	a.TimeStamp=tv.tv_sec;
	a.Name="Some name";
	a.UserName="Test User 1";

	f.Nbytes = 123321;
	f.FileName = strndup(fw_name, strlen(fw_name)+1);
	f.FuncName = strndup(fw_funcname, strlen(fw_funcname));

	d.TimeStamp = tv.tv_sec;
	d.DBName = "SQLite name";
	d.TableName = "AuditStorage";
	d.UserName = "admin";

	for (i=0; i<NMSGS; i++) {
		switch (i%3) {
			case 0:
			a.ConnID=i;
			addMonStats(AUDITEVENT, &a, addmsg, &reply_msg, &err_reply);
			break;

			case 1:
			f.ConnID=i;
			addMonStats(FILEWRITEEVENT, &f, addmsg, &reply_msg, &err_reply);
			break;

			case 2:
			d.ConnID=i;
			addMonStats(DBWRITEEVENT, &d, addmsg, &reply_msg, &err_reply);
			break;

			default:
			break;
		}
	}
	printf("After %dth getMonStats, counter : %s\n", i, reply_msg.p);
	sleep(20);
	return 1;
}
