#!/usr/bin/python 
import zkpython
import sys
import os

if __name__ == '__main__':
   zkc = zkpython.ZKClient('localhost:2181')
   if len(sys.argv)>0:
      value = zkc.get(sys.argv[1])
      print value
    
