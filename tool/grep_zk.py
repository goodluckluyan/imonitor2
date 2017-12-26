#!/usr/bin/python 
import zkpython
import sys
import os

if __name__ == '__main__':
   zkc = zkpython.ZKClient('localhost:2181')
   node_ls = ['/scheduler/server','/scheduler/task','/scheduler/agent','/scheduler/agent/sms']
   for node in node_ls:
	   value = zkc.get_children(node)
	   print '%s:%s'%(node,value)
	   for child_node in value:
	      path = '%s/%s'%(node,child_node)
	      child_node_val = zkc.get(path)
	      print '%s value:%s'%(path,child_node_val[0])
    
