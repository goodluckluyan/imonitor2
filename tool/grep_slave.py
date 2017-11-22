#!/usr/bin/python
import sys
import os
slave=['slave1','slave2','slave3']
for s in slave:
	grep_cmd = "ssh %s ps -ef|grep monitor_agent.py|grep -v 'grep monitor_agent.py'|awk '{print $2}'"%s
	p = os.popen(grep_cmd)
	pid = p.readline()
	print '%s(%s)'%(grep_cmd,pid[:-1])
	p.close()
	grep_sms_cmd = "ssh %s ps -ef|grep oristar|grep -v 'grep oristar'|awk '{print $8}'"%s
	p = os.popen(grep_sms_cmd)
	pid_l = p.readlines()
	for p in pid_l:
	  print 'sms: %s'%(p[-30:-1])

