#!/usr/bin/python
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
import spyne_webservice
import sys
import logging
import getopt
# from spyne_webservice import MainObj
# from agent_monitor import LocalhostRole
import StringIO
import traceback
import agent_monitor
import stat_manager
import master_monitor


from logging.handlers import RotatingFileHandler

# logging.basicConfig(level=logging.DEBUG,
#                     format='[%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
#                     datefmt='%a, %d %b %Y %H:%M:%S',
#                     filename='/usr/local/imonitor2/master.log',
#                     filemode='w')

# 定义一个StreamHandler，将INFO级别或更高的日志信息打印到标准错误，并将其添加到当前的日志处理对象#
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('[%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)

# 定义一个RotatingFileHandler，最多备份5个日志文件，每个日志文件最大10M
Rthandler = RotatingFileHandler('/usr/local/imonitor2/master.log', maxBytes=10 * 1024 * 1024, backupCount=5)
Rthandler.setLevel(logging.INFO)
formatter = logging.Formatter('[%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')
Rthandler.setFormatter(formatter)
logging.getLogger('').addHandler(Rthandler)

zk_server_ip = '127.0.0.1:2181'
servicedir = '/usr/local/imonitor2'
pidfile = '/var/run/imonitor_master.pid'
filelock = '/var/lock/subsys/imonitor_master'
server_node_name  = 'master1'

class LocationMgr:
    '''运行位置管理'''
    FIRST_HOST  = 0
    SECOND_HOST = 1
    THIRD_HOST  = 2
    RUN = 1
    STOP = 8

    def __init__(self,agent_ls,location_info_json,agent_node,loger):
        self.agent_ls = agent_ls
        self.agent_enable = {}
        self.agent_node = agent_node
        self.loger = loger
        for agent in agent_ls:
            self.agent_enable[agent] = False
        log = "LocationMgr agent enable ",self.agent_enable
        self.loger.info(log)

        self.location = location_info_json

    def disable_agent(self,agent_name):
        if agent_name not in self.agent_ls:
            return -1
        else:
           self.agent_enable[agent_name] = False
           return 0

    def enable_agent(self,agent_name):
        if agent_name not in self.agent_ls:
            return -1
        else:
            self.agent_enable[agent_name] = True
            return 0

    def get_sms_location(self,sms_name,prioirty,bcheck=True):
        '''按优先顺序查找启动主机,prioirty:优先顺序就是启动顺序 ,bcheck 是否检测运行状态'''
        if sms_name not in self.location.keys() :
            return ''
        else:
            if prioirty >=0 and prioirty <=2:
                agent = self.location[sms_name][prioirty]
                if bcheck and self.agent_enable[agent]:
                    return agent
                elif not bcheck:
                    return agent
                else:
                    return ''
            else:
                for agent in self.location[sms_name]:
                        if bcheck and self.agent_enable[agent]:
                            return agent
                        elif not bcheck:
                            return agent

    def get_agent_runsms(self,agent_name,prioirty=0,bcheck=True):
        '''获取主机上运行的sms的状态'''
        runsms={}
        if agent_name not in self.agent_ls:
            return []

        for sms in self.location.keys():
            if self.get_sms_location(sms,prioirty,bcheck) == agent_name:
                runsms[sms] = LocationMgr.RUN
            else :
                runsms[sms] = LocationMgr.STOP

        return runsms

    def get_sms_name(self):
        return self.location.keys()

    def get_agent_name(self):
        return self.agent_ls

    def get_agent_host_ip(self,agent_name):
        if agent_name in self.agent_ls:
            return self.agent_node[agent_name]
        else:
            log =  "not find agentnode %s"%agent_name
            self.loger.info(log)
            return ""

    def get_disalbe_host(self):
        dis_ls =[]
        for node in self.agent_enable:
            if not self.agent_enable[node]:
                dis_ls.append(node)

        return dis_ls




class AgentMgr:
    '''任务管理'''

    def __init__(self,agent_ls,localhost_name,locationmgr,zkctrl,regist_timeout,loger):
        self.agent_mgr = {}
        self.msg_queue = Queue(50)
        self.agent_ls = agent_ls
        self.zkctrl = zkctrl
        self.regist_timeout = regist_timeout
        self.process_th = Thread(target=self.process)
        self.agent_stat = stat_manager.StatMgr(agent_ls,locationmgr,self.msg_queue,loger)
        self.delay_switch_thread = Thread(target=self.delay_switch_fun)
        self.agent_regist_timeout = {}
        self.agent_cmd_txt = {}
        self.delay_switch = {}
        self.sms_state_transform=[]
        self.cond = threading.Condition()
        self.loger = loger
        self.localhost_name = localhost_name
        self.prevous_stat_matrix = '' #记录上次状态矩阵的状态

        for agent_name in agent_ls:
            self.agent_regist_timeout[agent_name] = 0

    def delay_switch_fun(self):
        while True:
            switch_ls = []
            if  self.cond.acquire():
                if len(self.sms_state_transform) == 0:
                    self.cond.wait()
                else:
                    for item in self.sms_state_transform:
                        sms_name = item.keys()
                        if sms_name not in self.delay_switch.keys():
                            continue
                        else:
                            if item[sms_name] == 101:
                                cur_host = self.delay_switch[sms_name][0]
                                new_host = self.delay_switch[sms_name][1]
                                switch_ls.append((sms_name,cur_host,new_host))

            self.cond.release()
            for switch in switch_ls:
                self.switch(switch[0], switch[1], switch[2])

            time.sleep(2)

    def start_monitor(self):
        for agent in self.agent_ls:
            self.agent_mgr[agent] = agent_monitor.MonitorAgent('/scheduler/agent', agent, self.msg_queue,
                                                               self.regist_timeout, self.zkctrl,self.localhost_name,self.loger)
            self.agent_mgr[agent].start_monitor()
        self.process_th.start()
        self.delay_switch_thread.start()


    def start_agent(self,ip):
        cmd = 'ssh root@%s "python /usr/local/imonitor2/monitor_agent.py 1>/dev/null &"'%ip
        log =  "start agent:%s"%cmd
        self.loger.info(log)
        os.system(cmd)

    def process(self):
        while True:
            msg = self.msg_queue.get()
            msg = msg.replace("'", '"').replace('u"','"')  # 去除u,替换单引号成双引号

            log =  "%s :get msg %s"%(sys._getframe().f_code.co_name,msg)
            self.loger.info(log)
            agent_stat = json.loads(msg)
            msg_name = agent_stat.keys()[0]
            agent_name = ''
            sms_name = ''
            if '@' in msg_name:                                 # sms消息：键值为agent_name@sms_name
                agent_name,sms_name = msg_name.split('@')
            else:                                               # agent消息
                agent_name = msg_name

            monitor_stat = agent_stat[msg_name]
            if agent_name in self.agent_ls and sms_name is '':   # slave状态
                if monitor_stat[0] == 'regist':                  # agent注册后给agent分派任务，并监听sms是否运行
                    self.agent_stat.update_agent_state(agent_name,'regist')
                    self.agent_regist_timeout[agent_name] = 0
                elif monitor_stat[0] == 'spawn':
                    runsms_json = '{"spawn":%s}'%monitor_stat[1]
                    runsms_json = runsms_json.replace('u','').replace("'",'"')#去除u,替换单引号成双引号
                    task_node = '/scheduler/task/%s' % agent_name
                    self.zkctrl.async('/scheduler/task')
                    if self.zkctrl.exists(task_node):
                        self.zkctrl.set(task_node,runsms_json)
                    else:
                        self.zkctrl.create(task_node, runsms_json,1)   # 下发任务,即更新task的状态
                    self.agent_cmd_txt[agent_name] = runsms_json       # 记录当前命令语句
		    
                    #找到是否有启动的sms
                    run_sms = json.loads(runsms_json)
                    is_run = False
                    for sms_id in run_sms['spawn']:
                        if run_sms['spawn'][sms_id] == 1:
                            is_run = True
                            break
                    sms_ls = []
                    if is_run:
                        sms_ls = self.zkctrl.get_children(u'/scheduler/agent/sms',
                                                     self.agent_mgr[agent_name].watcher_sms)     # 监听此agent下的字节点sms
                        self.agent_stat.update_agent_state(agent_name,'spawning',runsms_json)# 更新状态到spawning

                    #检测是否sms已经运行
                    if sms_ls:
                        for sms in sms_ls:
                            agt_id,sms_id = sms.split('@')
                            if agt_id == agent_name:
                                    sms_stat = self.zkctrl.get('/scheduler/agent/sms/%s' % (sms),
                                                               self.agent_mgr[agent_name].watcher_sms)
                                    self.agent_stat.update_sms_state(agent_name, sms_id, sms_stat[0])

                elif monitor_stat[0] == 'regist_timeout':
                    self.agent_regist_timeout[agent_name] += 1
                    if self.agent_regist_timeout[agent_name] == 3:                      #3检测超时则启动接管
                        self.agent_stat.update_agent_state(agent_name,'regist_timeout') #启动接管

                    host_ip = self.agent_stat.get_agent_host_ip(agent_name)         #尝试启动主机的agent
                    log =  "check agent %s:%s timeout ,so start it "%(agent_name,host_ip)
                    self.loger.info(log)

                    #始终检测
                    self.start_agent(agent_name)
                    self.agent_mgr[agent_name].restart_monitor(self.regist_timeout)

                elif monitor_stat[0] == 'change':
                    pass

                elif monitor_stat[0] == 'delete':
                    host_ip = self.agent_stat.get_agent_host_ip(agent_name)  # 尝试启动主机的agent
                    self.start_agent(agent_name) #先尝试启动主机的agent
                    self.agent_mgr[agent_name].restart_monitor(self.regist_timeout)

                elif monitor_stat[0] == 'child_change':
                    sms_ls = self.zkctrl.get_children('/scheduler/agent/sms')
                    rum_sms_ls = self.agent_stat.get_run_sms_in_agent(agent_name)
                    log = 'process::child_change:',sms_ls,rum_sms_ls
                    self.loger.info(log)
                    for sms in sms_ls:
                        agent_id, smsid =  sms.split('@')
                        if  agent_id == agent_name:                       #只处理属于本主机的  smsid not in rum_sms_ls and
                            sms_stat = self.zkctrl.get('/scheduler/agent/sms/%s'%(sms),self.agent_mgr[agent_name].watcher_sms)
                            self.agent_stat.update_sms_state(agent_name,smsid,sms_stat[0])
                elif monitor_stat[0] == 'take_over':
                    old_cmd_txt = self.agent_cmd_txt[agent_name]
                    take_sms = monitor_stat[1]
                    new_take_sms = "%s"%take_sms
                    new_take_sms = new_take_sms.replace('u','').replace("'",'"')
                    take_over_txt = '{%s,"take_over":%s}'%(old_cmd_txt[1:-1],new_take_sms)           #因为old_cmd_txt 是一个完成的json，所以去掉两端的大于号
                    self.zkctrl.set('/scheduler/task/%s' % agent_name, take_over_txt)                # 更新task的状态
                    self.zkctrl.get_children('/scheduler/agent/%s' % agent_name,
                                             self.agent_mgr[agent_name].watcher_sms)                 # 监听此agent下的字节点sms
                    self.agent_stat.update_agent_state(agent_name,'take_over','{"take_over":%s}'%take_sms)# 更新状态矩阵的takeover状态

                elif monitor_stat[0] == "shutdown_sms":
                    self.agent_stat.update_agent_state(agent_name,"switching")
                    agent_cmd_txt = self.agent_cmd_txt[agent_name]
                    agent_cmd = json.loads(agent_cmd_txt)
                    spawn_sms_dic = agent_cmd['spawn']

                    #当前情况下monitor_stat 是一个sms名称,所以不能用for sms in monitor_stat[1]这种形式
                    #for sms in monitor_stat[1]:
                    sms = monitor_stat[1]
                    print 'sms:%s'%sms
                    if sms in spawn_sms_dic:
                        spawn_sms_dic[sms] = LocationMgr.STOP

                        print 'spawn_sms_dic',spawn_sms_dic
                    if 'take_over' in agent_cmd.keys():
                        takeover_sms_dic = agent_cmd['take_over']
                        if sms in takeover_sms_dic:
                            spawn_sms_dic[sms] = LocationMgr.STOP

                    cmd = json.dumps(agent_cmd)
                    self.agent_cmd_txt[agent_name] = cmd
                    log =  "set /scheduler/task/%s:%s" % (agent_name, cmd)
                    self.loger.info(log)
                    self.zkctrl.set('/scheduler/task/%s' % agent_name,cmd)
                    self.agent_stat.update_agent_state(agent_name,"shutdown_sms",sms)

                elif monitor_stat[0] == 'startup_sms':
                    self.agent_stat.update_agent_state(agent_name,"switching")
                    if agent_name not in self.agent_cmd_txt.keys():
                       self.agent_cmd_txt[agent_name]=''
                    agent_cmd_txt = self.agent_cmd_txt[agent_name]
                    agent_cmd = json.loads(agent_cmd_txt)
                    spawn_sms_dic = agent_cmd['spawn']

                    #for sms in monitor_stat[1]:
                    sms = monitor_stat[1]
                    print 'sms:%s'%sms
                    if sms in spawn_sms_dic:
                        spawn_sms_dic[sms] = LocationMgr.RUN
                    if 'take_over' in agent_cmd.keys():
                        takeover_sms_dic = agent_cmd['take_over']
                        if sms in spawn_sms_dic:
                            spawn_sms_dic[sms] = LocationMgr.RUN

                    cmd = json.dumps(agent_cmd)
                    self.agent_cmd_txt[agent_name] = cmd
                    log =  "set /scheduler/task/%s:%s"%(agent_name,cmd)
                    self.loger.info(log)
                    self.zkctrl.set('/scheduler/task/%s' % agent_name, cmd)
                    self.agent_stat.update_agent_state(agent_name, "startup_sms", sms)

                elif monitor_stat[0] == 'switch_done':
                    log =  'agent % switch done '%agent_name
                    self.loger.info(log)

                elif monitor_stat[0] == 'backing_takeover':
                    switch_ls = monitor_stat[1]
                    log = '%s back ,start backing-takeover %s'%(agent_name,switch_ls)
                    self.loger.info(log)

                    #切换每个待恢复厅，发送切换命令
                    for bto_sms_id in switch_ls:
                        log = 'switch %s form %s to %s'%(bto_sms_id,switch_ls[bto_sms_id],agent_name)
                        self.loger.info(log)
                        self.switch(bto_sms_id,switch_ls[bto_sms_id],agent_name)

            elif agent_name in self.agent_ls  and sms_name in self.agent_stat.get_sms_name():#sms状态
                if monitor_stat[0] == 'change':  #sms 状态发生变化
                    new_stat = monitor_stat[1]
                    self.agent_stat.update_sms_state(agent_name,sms_name,new_stat)

                    self.cond.acquire()
                    self.sms_state_transform.append({sms_name:new_stat})
                    self.cond.notify()
                    self.cond.release()

                if monitor_stat[0] == 'delete':
                    self.agent_stat.update_sms_state(agent_name,sms_name,'delete')



            #消息处理完成
            self.msg_queue.task_done()
            self.update_master_zkstate()

    # 获取所有运行的sms的状态
    def get_all_run_sms(self):
        return self.agent_stat.get_all_run_sms_info()

    # 执行切换操作
    def switch(self,sms,cur_host,new_host):
        #print u'AgentMgr:switch'
        stat = self.agent_stat.get_sms_stat(cur_host, sms)
        log = 'switch %s ,cur stat %s'%(sms,stat)
        self.loger.info(log)
        if int(stat) == 101 or int(stat)==1:
            msg_txt = '{"%s":["shutdown_sms","%s"]}'%(cur_host,sms)
            self.msg_queue.put(msg_txt)
            msg_txt = '{"%s":["startup_sms","%s"]}' %(new_host, sms)
            self.msg_queue.put(msg_txt)

    # 把当前状态更新到/scheduler/server/masterx上，以便于和备机同步状态
    def update_master_zkstate(self):
        stat_matrix = self.agent_stat.get_stat_matrix()
        stat_matrix_txt = json.dumps(stat_matrix)
        if stat_matrix_txt != self.prevous_stat_matrix:
            self.zkctrl.set('/scheduler/server/%s'%self.localhost_name,stat_matrix_txt)
            self.prevous_stat_matrix = stat_matrix_txt

    # 在元节点接管时，恢复状态
    def set_stat_matrix(self,stat_matrix_json):
        stat_matrix = json.loads(stat_matrix_json)
        self.agent_stat.set_stat_matrix(stat_matrix)


    def getNodeHealthStat(self):
        dis_ls = self.agent_mgr.getDisableHost()
        if len(dis_ls)>0:
            return False
        else:
            return True


class MasterMgr:
    '''管理两台元服务器,实现高可用'''

    def __init__(self,localhost,otherhost,zkctrl,regist_timeout,ingest_sms_info,loger):
        self.zkctrl = zkctrl
        self.regist_timeout = regist_timeout
        self.loger = loger
        self.msg_queue = Queue(10)
        self.process_th = Thread(target=self.process)
        self.localhost = localhost
        self.otherhost = otherhost
        self.otherhost_regist_timeout = 0
        self.role = '' #角色有： leader,follower,onlyleader,takeover_leader
        self.otherhost_stat_matrix = ''


        # 开启导片sms
        self.ingest_dog = watchdog.watchctrl(ingest_sms_info['path'], ingest_sms_info['cmd'],
                                             ingest_sms_info['parameter'], ingest_sms_info['port'], loger,'ingest_sms')

        if localhost=='master1':
            self.role = 'onlyleader'
            agent_monitor.setLocalhostRole(self.role)
            self.ingest_dog.start()
        else:
            self.role = ''

    def set_setstatmatrix_fun(self,set_stat_matrix_fun):
        self.set_stat_matrix_fun = set_stat_matrix_fun

    def process(self):
        while True:
            msg = self.msg_queue.get()
            msg = msg.replace("'", '"').replace('u"', '"')  # 去除u,替换单引号成双引号

            log = "get msg %s" %msg
            self.loger.info(log)
            master_stat = json.loads(msg)
            msg_name = master_stat.keys()[0]
            agent_name = msg_name
            monitor_stat = master_stat[msg_name]
            if agent_name in self.otherhost :    # master状态
                if monitor_stat[0] == 'regist':  # master注册,根据当前角色转换到新的角色
                    if self.role == 'onlyleader':
                        self.role = 'leader'
                    elif self.role == '':
                        self.role = 'follower'
                    elif self.role == 'takeover_leader':
                        agent_monitor.setLocalhostRole('follower')
                        self.ingest_dog.stop()
                        self.ingest_dog.kill()
                        self.role == 'follower'
                    self.otherhost_regist_timeout = 0
                elif monitor_stat[0] == 'regist_timeout':
                    self.otherhost_regist_timeout += 1
                    if self.otherhost_regist_timeout == 3 and self.otherhost=='master1':  # 3检测超时则启动接管
                        #将leader的状态付给follower
                        if self.otherhost_stat_matrix:
                            stat_matrix = json.loads(self.otherhost_stat_matrix)
                            self.set_stat_matrix_fun(stat_matrix)
                        agent_monitor.setLocalhostRole('takeover_leader')
                        self.role = 'takeover_leader'
                        self.ingest_dog.start()
                    log = "check agent %s:%s timeout ,so start it " % (agent_name, self.otherhost)
                    self.loger.info(log)

                    # 始终检测
                    self.start_other_master(self.otherhost)
                    self.host_mgr.restart_monitor(self.regist_timeout)

                elif monitor_stat[0] == 'change':
                    if len(monitor_stat) == 2:
                        self.otherhost_stat_matrix = monitor_stat[1]


                elif monitor_stat[0] == 'delete':
                    # 始终检测
                    self.start_other_master(self.otherhost)
                    self.host_mgr.restart_monitor(self.regist_timeout)


                elif monitor_stat[0] == 'take_over':
                    pass

                elif monitor_stat[0] == 'backing_takeover':
                    pass




            # 消息处理完成
            self.msg_queue.task_done()

    # 开始对另一台元服务的监控
    def start_monitor(self):
        self.host_mgr = master_monitor.MonitorMaster('/scheduler/server', self.otherhost, self.msg_queue,
                                                               self.regist_timeout, self.zkctrl, self.localhost,
                                                               self.loger)
        self.host_mgr.start_monitor()
        self.process_th.start()

    # 通过ssh启动对端的master
    def start_other_master(self, ip):
        cmd = 'ssh root@%s "python /usr/local/imonitor2/monitor_master.py 1>/dev/null &"' % ip
        log = "start %s :%s" %(self.otherhost,cmd)
        self.loger.info(log)
        os.system(cmd)

# 成为守护进程
def daemonize(stdin='/dev/null', stdout='/dev/null', stderr='/dev/null'):
    '''Fork当前进程为守护进程，重定向标准文件描述符
        （默认情况下定向到/dev/null）'''

    # Perform first fork.
    try:
        pid = os.fork()
        if pid > 0:
            sys.exit(0)  # first parent out
    except OSError, e:
        sys.stderr.write("fork #1 failed: (%d) %s\n" % (e.errno, e.strerror))
        sys.exit(1)

    # 从母体环境脱离
    os.chdir(servicedir)
    os.umask(0)
    os.setsid()
    # 执行第二次fork
    try:
        pid = os.fork()
        if pid > 0:
            sys.exit(0)  # second parent out
    except OSError, e:
        sys.stderr.write("fork #2 failed: (%d) %s]n" % (e.errno, e.strerror))
        sys.exit(1)

    # 进程已经是守护进程了，重定向标准文件描述符
    for f in sys.stdout, sys.stderr:
        f.flush()
    si = file(stdin, 'r')
    so = file(stdout, 'a+')
    se = file(stderr, 'a+', 0)
    os.dup2(si.fileno(), sys.stdin.fileno())
    os.dup2(so.fileno(), sys.stdout.fileno())
    os.dup2(se.fileno(), sys.stderr.fileno())

def readpidfile(file):
    try:
        if os.path.exists(file):
            with open(file, 'r') as f:
                ret = int(f.readline())
            return ret
    except ValueError, ex:
        return 0


def writepidfile(file):
    with open(file, 'w+') as f:
        f.write("%s" % os.getpid())
    return

def usage():
    print 'sms_watchdog usage:'
    print '-h,--help: print help message.'

# 修饰主函数，用于异常中断的处理
def exemain(func):
    def wrapper(*args, **kw):
        try:
            func(*args, **kw)
        except Exception, ex:
            tm = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            log = "%s:%s\n" % (tm, ex)
            logging.info(log)
            os.remove(filelock)
        except KeyboardInterrupt, ex:
            tm = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            log = "%s:%s\n" % (tm, ex)
            logging.info(log)
            os.remove(filelock)
    return wrapper



@exemain
def main(args,loger):
    # try:
    #     opts, arg = getopt.getopt(args, "h", ["help"])
    # except getopt.GetoptError:
    #     usage()
    #     sys.exit(2)
    #
    # for opt, argitem in opts:
    #     if opt in ("-h", "help"):
    #         usage()
    #         sys.exit(0)
    #     else:
    #         usage()
    #         sys.exit(2)
  try :
    pid = 0
    bRun = False
    bFileExits = False
    runtm = time.time()
    zombiestatustm = 0
    if (os.path.exists(filelock)):
        bFileExits = True

    # 检证pid是不是在运行
    prepid = 0
    prepid = readpidfile(pidfile)
    if prepid != 0 and prepid != None:
        cmd = "ps %d" % prepid
        a = os.system(cmd)
        if a == 0:
            bRun = True
        else:
            bRun = False

    # 如果锁文件存在并且/var/run/imonitor_master.pid中记录的pid在运行，则不运行些脚本
    if bFileExits and bRun:
        print "Service Is Running<pid:%d>!" % prepid
        return

    # 成为守护进程
    #daemonize()

    # 装此进程的pid写到/var/run/oristar_sms_server.pid中
    writepidfile(pidfile)
    os.system('touch %s' % filelock)

    # 加载配置文件
    log = 'Monitor Master Start\n'
    log +='Loading config file'
    loger.info(log)
    cfg = {}
    with open('/usr/local/imonitor2/config.json', 'r') as config:
        cfg = json.load(config)
    agent_node = cfg['AgentNode']
    sms_run_host = cfg['RunHost']
    ingest_sms = cfg['Ingest_SMS']
    timeout = cfg['TimeOut']
    agent_ls = agent_node.keys()
    log =  "Load config successful,AgentNode %s"%agent_node
    loger.info(log)


    location_mgr = LocationMgr(agent_ls, sms_run_host,agent_node,loger)
    zkctl = zkpython.ZKClient(zk_server_ip)
    zkctl.async()
    localhost_name = cfg['NodeName']

    #  根据NodeName创建元服务节点
    server_node_name = '/scheduler/server/%s' %localhost_name
    loger.info( server_node_name)
    if not zkctl.exists(server_node_name):
        zkctl.create(server_node_name, "", 1)


    # 创建master管理实例，并启动对/scheduler/server节点的监控
    master_node = cfg['MasterNode']
    other_master_host = ''
    for host in master_node:
        if host == localhost_name:
            continue
        else:
            other_master_host = host

    regist_timeout = timeout['slaver_regist']

    master_mgr = MasterMgr(localhost_name, other_master_host, zkctl, regist_timeout, ingest_sms, loger)
    master_mgr.start_monitor()

    # 创建agent管理实例，并启动对/scheduler/agent节点的监控
    agent_mgr = AgentMgr(agent_ls,localhost_name,location_mgr, zkctl,
                         timeout['slaver_regist'],loger)
    agent_mgr.start_monitor()
    master_mgr.set_setstatmatrix_fun(agent_mgr.set_stat_matrix)



    # 开启webservice
    spyne_webservice.setMainObj(agent_mgr)
    webservice_th = Thread(target=spyne_webservice.webserverFun)
    webservice_th.start()

    while True:
        # 定时更新sms的状态
        time.sleep(2)
        zkctl.async('/scheduler')
        all_run_sms = agent_mgr.get_all_run_sms()
        if not all_run_sms:
            continue
        for sms_id in all_run_sms:
            spyne_webservice.g_sms_stat[sms_id][1] = all_run_sms[sms_id][1]
  except:
    fp = StringIO.StringIO()
    traceback.print_exc(file=fp)
    message = fp.getvalue()
    loger.info(message)

if __name__ == '__main__':
    try:
        loger = logging.getLogger('mylogger')
        loger.setLevel(logging.DEBUG)
        main(sys.argv[1:],loger)
    except:
        fp = StringIO.StringIO()
        traceback.print_exc(file=fp)
        message = fp.getvalue()
        loger.info(message)
    	












