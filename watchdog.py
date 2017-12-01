#!/usr/bin/python
#-*- coding:utf-8 -*-
'watch imonitor'
__author__='luyan'

import os
import sys
import time
import suds
from suds.xsd.doctor import Import
from suds.xsd.doctor import ImportDoctor
import threading
from suds.cache import NoCache
import urllib2
import subprocess
import signal

INIT = 0
RUNNING =  1   #0001
WATCHING = 2   #0010
RUNNINGANDWATCHING = 3 #0011
NOWATCH = 4            #0100
RUNNINGANDNOWATCH = 5  #0101
STOP = 8               #1000
STOPANDNOWATCH = 12    #1100

work_dir = '/usr/local/imonitor2'

class watchdog:
    '''看门狗类'''


    def __init__(self,path,exename,port,para ,loger,notify_fun,sms_name,b_fork=False):
        self.service_name = exename
        self.service_path = path
        self.configfilepath = para
        self.logfile =  '%s/watchdog.log'%work_dir
        self.reboottmfile = '%s/rebootontimer.tmp'%path
        self.grepcmd = "ps -ef|grep %s|grep -v \"grep %s\"|awk '{print $2}'"%(self.service_path + "/" + self.service_name,
                                                                              self.service_path + "/" + self.service_name)
        self.check_interval_sec = 3
        self.run_service_name = "%s/%s %s 1>/dev/null &"%(self.service_path, self.service_name, self.configfilepath)
        self.pid = 0
        self.prepid = 0
        self.bExit = False
        self.stat = STOP
        self.lock = threading.Lock()#运行状态锁
        self.ss_lock = threading.Lock()#sms状态锁
        self.sms_stat = 0
        self.wsport = port
        self.wsclient=''
        self.loger = loger
        self.bfork = b_fork
        self.first_reboot_tm = 3
        self.second_reboot_tm = 7
        self.sms_name = sms_name
        self.notify_fun = notify_fun


    def check_init_stat(self):
        ret = self.exeshell(self.grepcmd)
        if ret == '' or ret == 'error':
            return STOP
        else:
            self.pid = int(ret)
            return RUNNING



    def initWebService(self,wsport):
        try:
            imp = Import('http://www.w3.org/2001/XMLSchema')
            imp.filter.add('http://WebXml.com.cn/')
            doctor = ImportDoctor(imp)
            self.wsclient = suds.client.Client("http://127.0.0.1:%d/?wsdl" % wsport, doctor=doctor)
        except urllib2.URLError, ex:
            bexcept = True
            tm = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            log = "%s suds open webservice err: %s\n" % (tm, ex)
            self.loger.info(log)
            pass


    def getsmsstatus(self):
        try:
            ret=self.wsclient.service.GetWorkState_CS()
            #tm=time.strftime("%Y-%m-%d %H:%M:%S",time.localtime())
            #self.writelogfile("%s getsmsstatus info:%s\n"%(tm,ret.info))
            return int(ret.workstate)
            #return 101
        except Exception,ex:
            log="getsmsstatus(localhost:%d) failed: %s\n"%(self.wsport, ex)
            self.loger.info(log)
            return -1

    def is_can_reboot_sms(self):
        try:
            if self.wsclient:
                ret = self.wsclient.service.IsCanRebootSms()
                log = '%s IsScheduled:%s' % (self.sms_name, ret)
                self.loger.info(log)
                return ret

        except Exception, ex:
            log = " is_scheduled(localhost:%d) failed: %s\n" % (self.wsport, ex)
            self.loger.info(log)
            return False

    def isreboot(self):
        try:
            tm=time.localtime()
            if tm.tm_hour==self.first_reboot_tm or tm.tm_hour == self.second_reboot_tm:
                if os.path.exists(self.reboottmfile):
                    state=os.stat(self.reboottmfile)
                    fmtm=time.localtime(state.st_mtime)
                    if fmtm.tm_mon==tm.tm_mon and fmtm.tm_mday==tm.tm_mday:
                        return False

                smsstatus=self.getsmsstatus()
                if smsstatus < 0 or smsstatus !=101 :
                    return False
                os.system("touch %s"%self.reboottmfile)
                return True
            else:
                return  False
        except Exception,ex:
            tm=time.strftime("%Y-%m-%d %H:%M:%S",time.localtime())
            log="%s isreboot failed: %s\n"%(tm, ex)
            self.loger.info(log)
            return False


    def exeshell(self,shcmd):
        try:
            #print shcmd
            pip=os.popen(shcmd)
            ret=pip.readline()
            pip.close()
            return ret
        except Exception,ex:
            tm=time.strftime("%Y-%m-%d %H:%M:%S",time.localtime())
            log="%s :execute cmd %s failed: %s\n"%(tm,shcmd, ex)
            self.loger.info(log)
            return 'error'





    def exemain(self, func):
        def wrapper(*args, **kw):
            try:
                func(*args, **kw)
            except Exception, ex:
                tm = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                log = "%s:%s\n" % (tm, ex)
                self.loger.info(log)
                os.remove(self.filelock)
            except KeyboardInterrupt, ex:
                tm = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                log = "%s:%s\n" % (tm, ex)
                self.loger.info(log)
                os.remove(self.filelock)
        return wrapper


    def writelogfile(self, log):
        if (os.path.exists(self.logfile)):
            with open(self.logfile, "a") as f:
                f.write(log)
        else:
            with open(self.logfile, "w+") as f:
                f.write(log)

    def stopwatch(self):
        self.bExit = True

    def killprocess(self):
        if self.pid > 0:
            ret = os.system('kill -9 %d'%self.pid)
            if ret == 0 :
                self.pid = 0
                self.stat = STOP
                return True

        return False

    def getstat(self):
        self.lock.acquire()
        stat = self.stat
        self.lock.release()
        return stat

    def get_sms_stat(self):
        self.lock.acquire()
        stat = self.sms_stat
        self.lock.release()
        return stat

    def run_cmd(self,cmd):
        try:
            pid = os.fork()
            if pid == 0:
                signal.signal(signal.SIGCLD,signal.SIG_IGN)
                os.closerange(0,65535)
                os.system(cmd)
                signal.signal(signal.SIGCLD, signal.SIG_DFL)
                os.exit(0)
        except OSError, e:
            sys.stderr.write("fork #1 failed: (%d) %s\n" % (e.errno, e.strerror))
            sys.exit(1)

    #设置两次重启时间
    def set_reboot_time(self,first_reboot,second_reboot):
        log = '%s set reboot time 1:%d 2:%d'%(self.sms_name,first_reboot,second_reboot)
        self.loger.info(log)
        self.first_reboot_tm = first_reboot
        self.seconcd_reboot_tm = second_reboot



    def watchfun(self):
        runtm = time.time()
        self.bExit = False
        cnt = 0
        zombiestatustm = 0
        while not self.bExit:
            ret = self.exeshell(self.grepcmd)
            # writelogfile("check sms pid:%s\n"%ret)
            if ret == 'error':
                ret = -1

                self.lock.acquire()
                self.stat = STOP
                self.lock.release()
            if len(ret) == 0:
                ret = 0
                self.lock.acquire()
                self.stat = STOP
                self.lock.release()

            else:
                #控制输出频率
                cnt += 1
                if cnt == 60:
                    log = "%s:%d stat:%d" % (self.grepcmd, int(ret),self.sms_stat)
                    self.loger.info(log)
                    cnt = 0

                self.pid = int(ret)

                self.lock.acquire()
                self.stat = RUNNING
                self.lock.release()

            try:
                ret = int(ret)
            except ValueError, ex:
                ret = -1

            if self.notify_fun:
                self.notify_fun(ret,self.sms_stat)

            # writelogfile("ret %d,time interval:%d %d\n"%(ret,time.time(),runtm))
            if ret == 0:
                if time.time() - runtm < 2:
                    continue
                runtm = time.time()
                tm = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                log = "%s:Check Service Stop ,So Run It<%s>\n" % (tm, self.run_service_name)
                self.loger.info(log)
                if self.bfork:
                    self.run_cmd(self.run_service_name)
                else:
                    os.system(self.run_service_name)
                #subprocess.Popen(['%s/%s'%(self.service_path,self.service_name),self.configfilepath],shell=True)
                #self.loger.info(os.environ)
                os.system("touch %s" % self.reboottmfile)
                time.sleep(self.check_interval_sec * 8)
            else:
                checkzombie = False
                failcnt = 0
                bexcept = False
                if not self.wsclient:
                    try:
                        log =  "create wsclient(http://127.0.0.1:%d/?wsdl)....."%self.wsport
                        self.loger.info(log)
                        imp = Import('http://www.w3.org/2001/XMLSchema')
                        imp.filter.add('http://WebXml.com.cn/')
                        doctor = ImportDoctor(imp)
                        self.wsclient = suds.client.Client("http://127.0.0.1:%d/?wsdl" % self.wsport, doctor=doctor)
                    except urllib2.URLError, ex:
                        bexcept = True

                        pass

                while not bexcept :
                    self.ss_lock.acquire()
                    sms_stat = self.getsmsstatus()
                    self.ss_lock.release()
                    if sms_stat > 0:
                       self.sms_stat = sms_stat
                       break
                    failcnt += 1
                    if failcnt >= 3:
                        checkzombie = True
                        if zombiestatustm == 0:
                            zombiestatustm = time.time()
                        break

                if failcnt == 0:
                    zombiestatustm = 0

                if checkzombie and zombiestatustm != 0:
                    keepsec = time.time() - zombiestatustm
                    if keepsec > 120:
                        os.system("kill -9 %d" % ret)
                        continue

            is_scheduled = self.is_can_reboot_sms()
            if self.isreboot() and ret > 0 and is_scheduled:
                os.system("kill -9 %d" % ret)
                tm = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                self.loger.info("%s: routine reboot(kill -9 %d)\n" % (tm, ret))

            time.sleep(self.check_interval_sec)


class watchctrl:
    '''对看门狗进行控制'''
    def __init__(self,path,cmd,para,wsport,msg_queue,loger,sms_name,b_fork=False):
        self.loger = loger
        self.name = sms_name
        self.parent_msg_queue = msg_queue
        self.sms_pid = 0
        self.sms_stat = 0
        if self.parent_msg_queue:
            self.dog = watchdog(path, cmd, wsport, para, loger, self.notification, sms_name, b_fork)
        else:
            self.dog = watchdog(path, cmd, wsport, para, loger, None, sms_name, b_fork)
        self.stat = self.dog.check_init_stat() + NOWATCH

    # 用于watchdog的状态改变或pid改变的回调
    def notification(self,pid,state):
        log = '%s watchctrl rec msg %d %d'%(self.name,pid,state)
        self.loger.info(log)
        if self.parent_msg_queue:
            if pid <= 0 and self.sms_pid != 0:
                msg = '{"delete":"%s"}' % (self.name)
                self.parent_msg_queue.put(msg)
                self.sms_pid = 0
            elif pid > 0 and self.sms_pid == 0 :
                self.sms_stat = pid
                msg = '{"regist":"%s"}' % (self.name)
                self.parent_msg_queue.put(msg)
                self.sms_pid = pid

    def start(self):
        #self.wt.setDaemon(True)
        log =  "%s start watchdog thread"%self.name
        self.loger.info(log)
        self.wt = threading.Thread(target=self.dog.watchfun)
        self.wt.start()
        self.stat = WATCHING + RUNNING



    def stop(self):
        log = "%s stop watchdog thread"%self.name
        self.loger.info(log)
        self.dog.stopwatch()
        self.wt.join()
        log = "%s successful stop watch thread"%self.name
        self.loger.info(log)
        self.stat = NOWATCH


    def kill(self):
        log = "%s kill %d"%(self.name,self.dog.pid)
        self.loger.info(log)
        self.dog.killprocess()

    def restart(self):
        self.wt = threading.Thread(target=self.dog.watchfun)
        self.wt.start()
        self.stat = WATCHING

    def getstat(self):
        return self.dog.getstat()+self.stat

    def setstat(self,new_stat):
        log =  "%s new stat"%self.name,new_stat
        self.loger.info(log)
        if new_stat == self.getstat():
            pass
        if new_stat&RUNNING == RUNNING and  self.stat&NOWATCH == NOWATCH:
            self.start()
            log =  "start ..."
            self.loger.info(log)
        if new_stat&STOP == STOP and self.stat&RUNNING == RUNNING and self.stat&WATCHING == WATCHING:
            log =  "stop...."
            self.loger.info(log)
            self.stop()
            self.kill()
        if new_stat & STOP == STOP and self.stat&RUNNING == RUNNING and self.stat&NOWATCH == NOWATCH:
            self.kill()


    def get_sms_state(self):
        if self.dog.stat == RUNNING:
            stat = self.dog.get_sms_stat()
            return stat
        else:
            return -1

    def set_sms_reboot_time(self,first_reboot,second_reboot):
        self.dog.set_reboot_time(first_reboot,second_reboot)


if __name__ == '__main__':
    import logging

    from logging.handlers import RotatingFileHandler

    logging.basicConfig(level=logging.INFO,
                        format='[%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                        datefmt='%a, %d %b %Y %H:%M:%S',
                        filename='/usr/local/imonitor2/agent.log',
                        filemode='w')

    # 定义一个StreamHandler，将INFO级别或更高的日志信息打印到标准错误，并将其添加到当前的日志处理对象#
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter('[%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)

    # 定义一个RotatingFileHandler，最多备份5个日志文件，每个日志文件最大10M
    Rthandler = RotatingFileHandler('/usr/local/imonitor2/agent.log', maxBytes=10 * 1024 * 1024, backupCount=5)
    Rthandler.setLevel(logging.INFO)
    formatter = logging.Formatter('[%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')
    Rthandler.setFormatter(formatter)
    logging.getLogger('').addHandler(Rthandler)

    loger = logging.getLogger('mylogger')
    loger.setLevel(logging.DEBUG)

    wc = watchctrl('/usr/local/smsserver/bin','oristar_sms_server','/etc/smsconfig/sms_config.ini',12340,loger)
    wc.start()

    smswc = {}
    for i in range(10):
        w= watchctrl('/usr/local/smsserver/smsserver%d'%(i+1), 'oristar_sms_server', '/etc/smsconfig/sms_config%d.ini'%(i+1), 12340, loger)
        w.start()
        smswc[i+1] = w

    while True:
        time.sleep(1)


