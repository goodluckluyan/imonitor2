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
	if len(pid)>0 :
		kill_cmd = 'ssh %s kill -9 %s'%(s,pid[:-1])
		os.system(kill_cmd)
		print kill_cmd
	grep_sms_cmd = "ssh %s ps -ef|grep oristar|grep -v 'grep oristar'|awk '{print $2}'"%s
	p = os.popen(grep_sms_cmd)
	pid_l = p.readlines()
	print '%s : %s'%(grep_sms_cmd,pid_l)
	for sms_pid in pid_l:
	  print 'sms pid %s'%sms_pid[:-1]
	  kill_sms_cmd = 'ssh %s kill -9 %s'%(s,sms_pid[:-1])
	  os.system(kill_sms_cmd)
	  print kill_sms_cmd

