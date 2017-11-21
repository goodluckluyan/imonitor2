#!/usr/bin/env python2.7
# -*- coding: UTF-8 -*-

import zkpython
import os
import watchdog
from Queue import Queue
from threading import Thread
import threading
import json
import time
import zookeeper



class MonitorMaster:
    '''监听master健康状态和它的导片sms的状态'''

    def  __init__(self,path,mastername,msg,timeout,zkclt,server_node_name,loger):
        '''msg是队列对象，存放消息字符串，timeout是等待agent注册的时间'''
        self.msg_queue = msg
        self.mastername = mastername
        self.parent_path = path
        self.th = Thread(target=self.check_master,args=(timeout,))
        self.zkclt = zkclt #zkpython.ZKClient('%s:2181'%zk_server_ip)
        self.bexist = False
        self.stat = 'noregist'
        self.server_node_name = server_node_name
        self.loger = loger

    def start_monitor(self):
        self.th.start()


    def restart_monitor(self,timeout):
        self.th = Thread(target=self.check_master, args=(timeout,))
        self.th.start()


    def check_master(self,timeout):
        '''监听slaver是否注册'''
        # 开始监听/scheduler/agent/%hostname
        log = 'check %s whether regist  '%self.mastername
        self.loger.info(log)
        self.bexist = False
        tm_cnt = 0
        while not self.bexist and (timeout == 0 or tm_cnt <= timeout):
            tm_cnt += 1
            self.zkclt.async('/scheduler/server')
            child_node = self.zkclt.get_children('/scheduler/server')#检测agent注册
            if self.mastername in child_node:
               stat = self.zkclt.get('/scheduler/server/%s' % self.mastername, self.watcher)
               self.bexist = True
               self.stat = stat
               log = 'checked /scheduler/server/%s status:%d %s'%(self.mastername,len(stat),stat)
               self.loger.info(log)
               if stat[0]:
                   msg_txt = '{"%s":["regist",%s]}' % (self.mastername,stat[0])
               else:
                   msg_txt = '{"%s":["regist"]}' % (self.mastername)
               self.msg_queue.put(msg_txt) #交由上层AgentMgr.process处理
               break

            time.sleep(1)
        if not self.bexist and tm_cnt > timeout:
            msg_txt = '{"%s":["regist_timeout"]}' % (self.mastername)
            self.msg_queue.put(msg_txt)  # 交由上层AgentMgr.process处理



    def watcher(self,h_zk,w_type,w_stat,w_path):
        '''
        监听 agent/slaver 发送消息格式为 {agent:[state,value]} value只有当stat为change时才会有
        watch回调
        h_zk:就是我们创建连接之后的返回值,我试了下,发现好像指的是连接的一个索引值,以0开始
        w_type:事件类型,-1表示没有事件发生,1表示创建节点,2表示节点删除,3表示节点数据被改变,4表示子节点发生变化
        w_state:客户端的状态,0:断开连接状态,3:正常连接的状态,4认证失败,-112:过期啦
        w_path:这个状态就不用解释了,znode的路径
        '''

        log = "watche node change:t:%d s:%d p:%s"%(w_type,w_stat,w_path)
        self.loger.info(log)
        if w_type == 3:
            new_vaule = zookeeper.get(h_zk, w_path, self.watcher)
            self.stat = new_vaule
            msg_txt = '{"%s":["change",%s]}'%(self.mastername,new_vaule[0])
            self.msg_queue.put(msg_txt)  # 交由上层AgentMgr.process处理


        elif w_type ==2:
            msg_txt = '{"%s":["delete"]}' % (self.mastername)
            self.msg_queue.put(msg_txt)  # 交由上层AgentMgr.process处理
            self.loger.info(msg_txt)


    def get_agent_stat(self):
        if self.bexist:
            return self.agent_stat

