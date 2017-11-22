#!/usr/bin/env python2.7
# -*- coding: UTF-8 -*-


import suds
import zkpython
import os
from Queue import Queue
from threading import  Thread
import threading
import json
import time
import zookeeper
import watchdog

import logging
import traceback
import StringIO
from logging.handlers import RotatingFileHandler


#logging.basicConfig(level=logging.WARNING,
#                    format='[%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
#                    datefmt='%a, %d %b %Y %H:%M:%S',
#                    filename='/usr/local/imonitor2/agent.log',
#                    filemode='w')

# 定义一个StreamHandler，将INFO级别或更高的日志信息打印到标准错误，并将其添加到当前的日志处理对象#
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('[%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')
console.setFormatter(formatter)
logging.getLogger('mylogger').addHandler(console)

# 定义一个RotatingFileHandler，最多备份5个日志文件，每个日志文件最大10M
Rthandler = RotatingFileHandler('/usr/local/imonitor2/agent.log', maxBytes=10 * 1024 * 1024, backupCount=5)
Rthandler.setLevel(logging.INFO)
formatter = logging.Formatter('[%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')
Rthandler.setFormatter(formatter)
logging.getLogger('mylogger').addHandler(Rthandler)


sms_path={
           'sms1':['/usr/local/smsserver/smsserver1','oristar_sms_server','/etc/smsconfig/sms_config1.ini',9001],
           'sms2':['/usr/local/smsserver/smsserver2','oristar_sms_server','/etc/smsconfig/sms_config2.ini',9002],
           'sms3':['/usr/local/smsserver/smsserver3','oristar_sms_server','/etc/smsconfig/sms_config3.ini',9003],
           'sms4':['/usr/local/smsserver/smsserver4','oristar_sms_server','/etc/smsconfig/sms_config4.ini',9004],
           'sms5':['/usr/local/smsserver/smsserver5','oristar_sms_server','/etc/smsconfig/sms_config5.ini',9005],
           'sms6':['/usr/local/smsserver/smsserver6','oristar_sms_server','/etc/smsconfig/sms_config6.ini',9006],
           'sms7':['/usr/local/smsserver/smsserver7','oristar_sms_server','/etc/smsconfig/sms_config7.ini',9007],
           'sms8':['/usr/local/smsserver/smsserver8','oristar_sms_server','/etc/smsconfig/sms_config8.ini',9008],
           'sms9':['/usr/local/smsserver/smsserver9','oristar_sms_server','/etc/smsconfig/sms_config9.ini',9009],
           'sms10':['/usr/local/smsserver/smsserver10','oristar_sms_server','/etc/smsconfig/sms_config10.ini',9010]
           }

zk_server_ip = '127.0.0.1:2181'
#zk_server_ip = '10.7.75.60:2181'
#sms_path = {'iostat':['/usr/bin','iostat','1','12340']}

class LogAndMgr:
     loger=None
     Mgr = None

def setLogAndMgr(loger,mgr):
    LogAndMgr.loger = loger
    LogAndMgr.Mgr = mgr

def getLogAndMgr():
    return (LogAndMgr.loger,LogAndMgr.Mgr)

class EventMgr:
    '''事件管理，事件触发和分派'''
    def __init__(self,hostname,maxtasknum,zkctrl,loger):
        self.task_queue = Queue(maxtasknum)
        self.task = {}
        self.hostname = hostname
        self.lock = threading.Lock()
        self.sms_stat = {}
        self.sms = {}
        self. zkctrl = zkctrl
        self.loger = loger
        for hallid in sms_path:
            sms_info = sms_path[hallid]
            self.sms[hallid] = watchdog.watchctrl(sms_info[0],sms_info[1],sms_info[2],sms_info[3],loger,hallid)
            self.sms_stat[hallid] = 0

    def process(self):
        while True:
            # 获取/scheduler/task/%hostname的最新状态，如果队列无数据则阻塞
            task_str = self.task_queue.get()

            #根据新状态改变看门狗状态,task状态格式为'{'sms1':1,'sms2':8}'
           
            self.task = json.loads(task_str[0])
            log =  "receive msg %s ,task %s" % (task_str, self.task)
            self.loger.info(log)
            self.lock.acquire()

            new_stat={}
            for cmd in self.task:
                # if cmd == 'spawn':
                #     if self.task[cmd][hallid] == 1:
                #         path = '/scheduler/agent/sms/%s@%s' % (self.hostname, hallid)
                #         if not self.zkctrl.exists(path):
                #             log = "create path zk:%s" % path
                #             self.loger.info(log)
                #             self.zkctrl.create(path, '%d' % self.task[cmd][hallid], 1)
                #         self.sms[hallid].setstat(self.task[cmd][hallid])
                #
                #     elif self.task[cmd][hallid] == 8:
                #         path = '/scheduler/agent/sms/%s@%s' % (self.hostname, hallid)
                #         if self.zkctrl.exists(path):
                #             log = 'delete path zk:%s' % path
                #             self.loger.info(log)
                #             self.zkctrl.delete(path)
                #         self.sms[hallid].setstat(self.task[cmd][hallid])
                # if cmd == 'take_over':
                #     for hallid in self.task[cmd]:
                #         if self.task[cmd][hallid] == 1:
                #             path = '/scheduler/agent/sms/%s@%s' % (self.hostname,hallid)
                #             if not self.zkctrl.exists(path):
                #                 log =  "create path zk:%s"%path
                #                 self.loger.info(log)
                #                 self.zkctrl.create(path, '%d' %self.task[cmd][hallid], 1)
                #             self.sms[hallid].setstat(self.task[cmd][hallid])

                # apawn和takeover中只要有个一个是1,则启动
                log = 'task(%s:%s)'%(cmd,self.task[cmd])
                self.loger.info(log)
                for hallid in self.task[cmd]:
                    log = 'new_stat: %s'%new_stat
                    self.loger.info(log)
                    if hallid in new_stat.keys():
                        if self.task[cmd][hallid] == 1 or new_stat[hallid] == 1:
                            new_stat[hallid] = 1
                        else:
                            new_stat[hallid] = 8
                    else:
                        new_stat[hallid] = self.task[cmd][hallid]


            log = 'cur run stat %s'%new_stat
            self.loger.info(log)
            for hallid in new_stat:
                if new_stat[hallid] == 1:
                    path = '/scheduler/agent/sms/%s@%s' % (self.hostname, hallid)
                    if not self.zkctrl.exists(path):
                        log = "create path zk:%s" % path
                        self.loger.info(log)
                        self.zkctrl.create(path, '%d' % new_stat[hallid], 1)
                    self.sms[hallid].setstat(new_stat[hallid])

                elif new_stat[hallid] == 8:
                    path = '/scheduler/agent/sms/%s@%s' % (self.hostname, hallid)
                    if self.zkctrl.exists(path):
                        log = 'delete path zk:%s' % path
                        self.loger.info(log)
                        self.zkctrl.delete(path)
                    self.sms[hallid].setstat(new_stat[hallid])

            self.lock.release()
            self.task_queue.task_done()

    def addmsg(self,msg):
        log = 'add msg',msg
        self.loger.info(log)
        self.task_queue.put(msg)

    def update_sms_stat(self,zk):
        new_sms_stat={}
        self.lock.acquire()
        for hallid in self.sms:
            stat = self.sms[hallid].get_sms_state()
            if int(stat) > 100:
                new_sms_stat[hallid] = stat
        self.lock.release()

        for hallid in new_sms_stat:
            if new_sms_stat[hallid] != self.sms_stat[hallid]:
                log = '%s stat change(%s->%s) '%(hallid,self.sms_stat[hallid],new_sms_stat[hallid])
                self.loger.info(log)
                path = '/scheduler/agent/sms/%s@%s' % (self.hostname,hallid)
                self.sms_stat[hallid] = new_sms_stat[hallid]
                if zk.exists(path):
                    log =  'set %s'%path,new_sms_stat[hallid]
                    self.loger.info(log)
                    zk.set(path,'%s'%new_sms_stat[hallid])



    def start(self):
        self.h_thread = Thread(target = self.process)
        self.h_thread.start()

    def stop(self):
        self.task_queue.join()




def watcher(h_zk,w_type,w_stat,w_path):
    '''
      watch回调
      h_zk:就是我们创建连接之后的返回值,我试了下,发现好像指的是连接的一个索引值,以0开始
      w_type:事件类型,-1表示没有事件发生,1表示创建节点,2表示节点删除,3表示节点数据被改变,4表示子节点发生变化
      w_state:客户端的状态,0:断开连接状态,3:正常连接的状态,4认证失败,-112:过期啦
      w_path:这个状态就不用解释了,znode的路径
    '''
    loger,mgr = getLogAndMgr()
    log = "watche node change:t:%d s:%d p:%s" % (w_type, w_stat, w_path)
    loger.info(log)
    if w_type == 3:
       try:
           new_value = zookeeper.get(h_zk, w_path, watcher)
           mgr.addmsg(new_value)
           log = 'put new value(%s) into queue'%new_value
           loger.info(log)
       except Exception,err:
           loger.info(err)
    if w_type == 2:
       parent_name = os.path.dirname(w_path)
       zookeeper.get_children(h_zk, parent_name,children_watcher)



def children_watcher(h_zk,w_type,w_stat,w_path):
    '''
       watch回调
       h_zk:就是我们创建连接之后的返回值,我试了下,发现好像指的是连接的一个索引值,以0开始
       w_type:事件类型,-1表示没有事件发生,1表示创建节点,2表示节点删除,3表示节点数据被改变,4表示子节点发生变化
       w_state:客户端的状态,0:断开连接状态,3:正常连接的状态,4认证失败,-112:过期啦
       w_path:这个状态就不用解释了,znode的路径
    '''
    loger,mgr = getLogAndMgr()
    log = "children_watcher node change:t:%d s:%d p:%s" % (w_type, w_stat, w_path)
    loger.info(log)
    hostname = mgr.hostname
    if w_type==4:
       child_ls = zookeeper.get_children(h_zk,w_path,children_watcher)
       log = "find child:",child_ls,"local host:",hostname
       loger.info(log)
    if hostname in child_ls:
       value = zookeeper.get(h_zk,'%s/%s'%(w_path,hostname),watcher)
       log =  "host %s,value:%s"%(hostname,value)
       loger.info(log)
       mgr.addmsg(value)


def init(zkclt,loger):
    '''agent 初始化'''
    hostname = getHostName()
    name_ls = hostname.split('-')

    # 由于hostname是\n结尾，\n会是zkclt.create出现bad argments的异常，所以去掉\n
    agent_node = '/scheduler/agent/%s'%name_ls[0][:-1]

    #根据hostname创建节点
    try:
        #zkclt.async(agent_node)
        log =  "create node %s"%agent_node
        loger.info(log)
        zkclt.create(path = agent_node,data = "",flags = 1)#创建临时节点
        return True

    except zookeeper.NodeExistsException , e:
        log =  "%s node had  exist:"%(agent_node) , e
        loger.info(log)
        return False
    except zookeeper.NoNodeException , e:
        log = "%s parent node not exist:"%agent_node , e
        loger.info(log)
        return False

def getHostName():
    '''获取主机的名字'''
    cmd = 'hostname'
    pf = os.popen(cmd)
    lines = pf.readlines()
    return lines[0]



def readpidfile(pidfile):
    '''读pid文件'''
    try:
        if os.path.exists(pidfile):
            with open(pidfile, 'r') as f:
                ret = int(f.readline())
            return ret
    except ValueError, ex:
        return 0


def writepidfile(pidfile):
    '''写pid文件'''
    with open(pidfile, 'w+') as f:
        f.write("%s" % os.getpid())
    return




if __name__ == '__main__':
 try:  
    #检测文件锁，防止起多个进程
    filelock = '%s/watchdog.lock'%watchdog.work_dir
    pidfile = '%s/watchdog.pid'%watchdog.work_dir
    bRun = False
    bFileExits = False

    if (os.path.exists(filelock)):
        bFileExits = True

    # 检证pid是不是在运行
    prepid = readpidfile(filelock)
    if prepid != 0 and prepid != None:
        cmd = "ps %d" % prepid
        a = os.system(cmd)
        if a == 0:
            bRun = True
        else:
            bRun = False

    
    loger = logging.getLogger('mylogger')
    loger.setLevel(logging.DEBUG)
    
    # 如果锁文件存在并且记录的pid在运行，则不运行些脚本
    if bFileExits and bRun:
        log =  "Service Is Running<pid:%d>!" % prepid
        loger.info(log)
        exit(0)



    # 装此进程的pid写到pid file中
    writepidfile(filelock)
    os.system('touch %s' % filelock)
    loger.info('Monitor Agent Start....')
    hostname = getHostName()
    name_ls = hostname.split('-')
    hostname = name_ls[0][:-1]

    zkclt = zkpython.ZKClient(zk_server_ip)
    if not init(zkclt,loger):
        exit(0)

    #开启事件处理
    MainMgr = EventMgr(hostname,10,zkclt,loger)
    setLogAndMgr(loger,MainMgr)
    MainMgr.start()


    # 开始监听/scheduler/task/%hostname
    child_node = zkclt.get_children('/scheduler/task',children_watcher)  # 查看任务
    log = '/scheduler/task child is:',child_node
    loger.info(log)
    log =  'hostname %s'%hostname
    loger.info(log)
    if hostname in child_node:
        task = zkclt.get('/scheduler/task/%s' % hostname, watcher)
        log = 'zookeeper get info :%s'%task[0]
        loger.info(log)
        MainMgr.addmsg(task)


    while True:
        try:
            MainMgr.update_sms_stat(zkclt)
            time.sleep(1)
            zkclt.async('/scheduler')
        except KeyboardInterrupt,e:
            loger.info( 'receive KeyboardInterrupt.')
 except:
     fp = StringIO.StringIO()
     traceback.print_exc(file=fp)
     message = fp.getvalue()
     loger.info(message)








