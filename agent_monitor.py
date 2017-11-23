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


class LocalhostRole:
    role = None


def getLocalhostRole():
    return LocalhostRole.role


def setLocalhostRole(new_role):
    LocalhostRole.role = new_role


class MonitorAgent:
    '''监听agent健康状态和它的sms的状态'''

    def  __init__(self,path,agentname,msg,timeout,zkclt,server_node_name,loger):
        '''msg是队列对象，存放消息字符串，timeout是等待agent注册的时间'''
        self.msg_queue = msg
        self.agent_name = agentname
        self.parent_path = path
        self.th = Thread(target=self.check_agent,args=(timeout,))
        self.sms_th ={}
        self.zkclt = zkclt #zkpython.ZKClient('%s:2181'%zk_server_ip)
        self.bexist = False
        self.sms_bexist={}
        self.agent_stat = 'noregist'
        self.server_node_name = server_node_name
        self.loger = loger

    def start_monitor(self):
        self.th.start()


    def restart_monitor(self,timeout):
        self.th = Thread(target=self.check_agent, args=(timeout,))
        self.th.start()

    def start_monitor_sms(self,sms_name,timeout):
        self.sms_th[sms_name] = Thread(target=self.check_sms,args=(sms_name,timeout,))
        self.sms_th[sms_name].start()

    def check_sms(self,sms_name,timeout):
        '''监听slaver@sms是否注册'''
        # 开始监听/scheduler/agent/sms/slave@sms
        self.sms_bexist[sms_name] = False
        tm_cnt = 0
        while not self.sms_bexist[sms_name] and (timeout == 0 or tm_cnt <= timeout):
            tm_cnt += 1
            self.zkclt.async('/scheduler/agent/sms')
            child_node = self.zkclt.get_children('/scheduler/agent/sms')  # 检测agent注册
            if sms_name in child_node:
               node_name =  '/scheduler/agent/sms/%s' %sms_name
               stat = self.zkclt.get(node_name)
               if int(stat[0])>100:#只有返回状态合格才算注册成功
                   self.sms_bexist[sms_name] = True
                   msg_txt = '{"%s":["regist"]}' % (sms_name)
                   cur_role = getLocalhostRole()
                   log = 'cur role %s' % cur_role
                   self.loger.info(log)
                   if cur_role and cur_role !='follower' :
                        self.msg_queue.put(msg_txt) #交由上层AgentMgr.process处理
                   else:
                        self.loger.info(msg_txt)
                   break

            time.sleep(1)
        if not self.sms_bexist[sms_name] and tm_cnt > timeout:
            msg_txt = '{"%s":["regist_timeout"]}' % (sms_name)
            cur_role = getLocalhostRole()
            log = 'cur role %s' % cur_role
            self.loger.info(log)
            if cur_role and cur_role != 'follower':
                self.msg_queue.put(msg_txt)  # 交由上层AgentMgr.process处理
            else:
                self.loger.info(msg_txt)


    def check_agent(self,timeout):
        '''监听slaver是否注册'''
        # 开始监听/scheduler/agent/%hostname
        self.bexist = False
        tm_cnt = 0
        while not self.bexist and (timeout == 0 or tm_cnt <= timeout):
            tm_cnt += 1
            self.zkclt.async('/scheduler/agent')
            child_node = self.zkclt.get_children('/scheduler/agent')  # 检测agent注册
            if self.agent_name in child_node:
               node_name =  '/scheduler/agent/%s' % self.agent_name
               stat = self.zkclt.get(node_name, self.watcher)
               log = 'add watcher to %s node'%node_name
               self.loger.info(log)

               self.bexist = True
               self.agent_stat = stat
               msg_txt = '{"%s":["regist"]}' % (self.agent_name)
               cur_role = getLocalhostRole()
               log = 'cur role %s' % cur_role
               self.loger.info(log)
               if cur_role and cur_role !='follower' :
                    self.msg_queue.put(msg_txt) #交由上层AgentMgr.process处理
               else:
                    self.loger.info(msg_txt)
               break

            time.sleep(1)
        if not self.bexist and tm_cnt > timeout:
            msg_txt = '{"%s":["regist_timeout"]}' % (self.agent_name)
            cur_role = getLocalhostRole()
            log = 'cur role %s' % cur_role
            self.loger.info(log)
            if cur_role and cur_role != 'follower':
                self.msg_queue.put(msg_txt)  # 交由上层AgentMgr.process处理
            else:
                self.loger.info(msg_txt)

    def watcher_sms(self,h_zk,w_type,w_stat,w_path):
        '''
        监听 agent/slaverX/smsX
        watch回调
        h_zk:就是我们创建连接之后的返回值,我试了下,发现好像指的是连接的一个索引值,以0开始
        w_type:事件类型,-1表示没有事件发生,1表示创建节点,2表示节点删除,3表示节点数据被改变,4表示子节点发生变化
        w_state:客户端的状态,0:断开连接状态,3:正常连接的状态,4认证失败,-112:过期啦
        w_path:这个状态就不用解释了,znode的路径
        '''
        sms_name = os.path.basename(w_path)
        log =  "%s watche sms change:t:%d s:%d p:%s" % (self.server_node_name,w_type, w_stat, w_path)
        self.loger.info(log)
        if w_type == 3:
            new_vaule = zookeeper.get(h_zk, w_path, self.watcher_sms)
            self.agent_stat = new_vaule
            msg_txt = '{"%s":["change","%s"]}' % (sms_name, new_vaule[0])
            cur_role = getLocalhostRole()
            log = 'cur role %s' % cur_role
            self.loger.info(log)
            if cur_role and cur_role != 'follower':
                self.msg_queue.put(msg_txt)  # 交由上层AgentMgr.process处理
            else:
                self.loger.info(msg_txt)


        elif w_type == 2:
            msg_txt = '{"%s":["delete"]}' % (sms_name)
            cur_role = getLocalhostRole()
            log = 'cur role %s' % cur_role
            self.loger.info(log)
            if cur_role and cur_role != 'follower':
                self.msg_queue.put(msg_txt)  # 交由上层AgentMgr.process处理
            else:
                self.loger.info(msg_txt)

        elif w_type == 4:#如果等于4，则sms_name 的内容为‘sms’，不能使用sms_name
            sms_ls =  zookeeper.get_children(h_zk,w_path,self.watcher_sms)
            bexist = False
            #检查是否存在属于此主机下的sms
            for sms in sms_ls:
                agent,smsid=sms.split('@')
                if agent == self.agent_name:
                    bexist = True
            if bexist:
                msg_txt = '{"%s":["child_change"]}'%self.agent_name
                cur_role = getLocalhostRole()
                log = 'cur role %s' % cur_role
                self.loger.info(log)
                if cur_role and cur_role != 'follower':
                    self.msg_queue.put(msg_txt)  # 交由上层AgentMgr.process处理
                else:
                    self.loger.info(msg_txt)


    def watcher(self,h_zk,w_type,w_stat,w_path):
        '''
        监听 agent/slaver 发送消息格式为 {agent:[state,value]} value只有当stat为change时才会有
        watch回调
        h_zk:就是我们创建连接之后的返回值,我试了下,发现好像指的是连接的一个索引值,以0开始
        w_type:事件类型,-1表示没有事件发生,1表示创建节点,2表示节点删除,3表示节点数据被改变,4表示子节点发生变化
        w_state:客户端的状态,0:断开连接状态,3:正常连接的状态,4认证失败,-112:过期啦
        w_path:这个状态就不用解释了,znode的路径
        '''

        log = "%s watche node change:t:%d s:%d p:%s"%(self.server_node_name,w_type,w_stat,w_path)
        self.loger.info(log)
        if w_type == 3:
            new_vaule = zookeeper.get(h_zk, w_path, self.watcher)
            self.agent_stat = new_vaule
            msg_txt = '{"%s":["change","%s"]}'%(self.agent_name,new_vaule)
            cur_role = getLocalhostRole()
            log = 'cur role %s'%cur_role
            self.loger.info(log)
            if cur_role and cur_role != 'follower':
                self.msg_queue.put(msg_txt)  # 交由上层AgentMgr.process处理
            else:
                self.loger.info(msg_txt)

        elif w_type ==2:
            msg_txt = '{"%s":["delete"]}' % (self.agent_name)
            cur_role = getLocalhostRole()
            log = 'cur role %s' % cur_role
            self.loger.info(log)
            if cur_role and cur_role != 'follower':
                self.msg_queue.put(msg_txt)  # 交由上层AgentMgr.process处理
            else:
                self.loger.info(msg_txt)


    def get_agent_stat(self):
        if self.bexist:
            return self.agent_stat

