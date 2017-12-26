#!/usr/bin/python
#-*- coding:utf-8 -*-

from spyne import Application, rpc, ServiceBase
from spyne import Integer, Unicode, Array, ComplexModel
from spyne.model.complex import Iterable
from spyne.protocol.soap import Soap11
from spyne.server.wsgi import WsgiApplication
from wsgiref import simple_server
import SocketServer

import sys
import logging


ip = '0.0.0.0'
port = 8851
g_sms_location = {'sms1': ['1','10.7.75.62', 9001],'sms2': ['2','10.7.75.62', 9002],'sms3': ['3','10.7.75.62', 9003],
                   'sms4': ['4','10.7.75.63', 9004],'sms5': ['5','10.7.75.63', 9005],'sms6': ['6','10.7.75.63', 9006],
                  'sms7': ['7','10.7.75.64', 9007],'sms8': ['8','10.7.75.64', 9008],'sms9': ['9','10.7.75.64', 9009],
                   'sms10': ['10', '10.7.75.64', 9010]}

g_sms_stat = {'sms1': ['1','10.7.75.62',9001],'sms2': ['2','10.7.75.62',9002],'sms3': ['3','10.7.75.62',9003],
               'sms4': ['4','10.7.75.63',9004],'sms5': ['5','10.7.75.63',9005],'sms6': ['6','10.7.75.63',9006],
               'sms7': ['7','10.7.75.64',9007],'sms8': ['8','10.7.75.64',9008],'sms9': ['9','10.7.75.64',9009],
               'sms10': ['10', '10.7.75.64', 9010]}

#g_sms_location = {'sms1': ['1','172.23.140.152', 9001],'sms2': ['2','172.23.140.152', 9002],'sms3': ['3','172.23.140.152', 9003],
#                  'sms4': ['4','172.23.140.93', 9004],'sms5': ['5','172.23.140.93', 9005],'sms6': ['6','172.23.140.93', 9006],
#                  'sms7': ['7','172.23.140.98', 9007],'sms8': ['8','172.23.140.98', 9008],'sms9': ['9','172.23.140.98', 9009],
#                  'sms10': ['10', '172.23.140.98', 9010]}

#g_sms_stat = {'sms1': ['1','172.23.140.152',9001],'sms2': ['2','172.23.140.152',9002],'sms3': ['3','172.23.140.152',9003],
#              'sms4': ['4','172.23.140.93',9004],'sms5': ['5','172.23.140.93',9005],'sms6': ['6','172.23.140.93',9006],
#              'sms7': ['7','172.23.140.98',9007],'sms8': ['8','172.23.140.198',9008],'sms9': ['9','172.23.140.98',9009],
#              'sms10':['10','172.23.140.98',9010]}

g_disk_stat = {'10.7.75.60': ['10.7.75.60', 1099511627776, 536870912, 536870912],
               '10.7.75.61': ['10.7.75.61', 1099511627776, 536870912, 536870912],
               '10.7.75.62': ['10.7.75.62', 1099511627776, 536870912, 536870912],
               '10.7.75.63': ['10.7.75.63', 1099511627776, 536870912, 536870912],
               '10.7.75.64': ['10.7.75.64', 1099511627776, 536870912, 536870912]}

g_eth_stat =  {'10.7.75.60': ['10.7.75.60', 102400,10240],
               '10.7.75.61': ['10.7.75.61', 102400,10240],
               '10.7.75.62': ['10.7.75.62', 102400,10240],
               '10.7.75.63': ['10.7.75.63', 102400,10240],
               '10.7.75.64': ['10.7.75.64', 102400,10240]}

class MainObj:
    AgentMgr = None
    DB_Status = -1
    HDFS_stat = -1
    HDFS_DeadNode = []

def setMainObj(obj):
    MainObj.AgentMgr = obj

def getMainObj():
    return MainObj.AgentMgr

def setDBSyncStat(db_stat):
    MainObj.DB_Status = db_stat

def getDBSyncStat():
    return MainObj.DB_Status

def setHDFSStat(hdfs_stat):
    MainObj.HDFS_stat = hdfs_stat

def getHDFSStat():
    return MainObj.HDFS_stat

def setHDFSDeadNode(deadnode):
    MainObj.HDFS_DeadNode = deadnode

def getHDFSDeadNode():
    return MainObj.HDFS_DeadNode

class sms_loc_info(ComplexModel):
    hallid = Unicode(nillable=False)
    ip = Unicode(nillable=False)
    port = Integer(nillable=False)

class sms_stat(ComplexModel):
    hallid = Unicode(nillable=False)
    location = Unicode(nillable=False)
    port = Integer(nillable=False)

class disk_stat(ComplexModel):
    hostid=Unicode(nillable=False)
    sumspace=Integer(nillable=False)
    used=Integer(nillable=False)
    unused=Integer(nillable=False)

class eth_stat(ComplexModel):
    hostid = Unicode(nillable=False)
    bandwidth = Integer(nillable=False)
    usage = Integer(nillable=False)


class IMonitorWebServices(ServiceBase):
    @rpc(_returns=Array(sms_loc_info))
    def getHalls(self):
        a = []
        key_ls = g_sms_stat.keys()
        key_ls.sort()
        for i in key_ls:
             a.append(g_sms_stat[i])
        return a


    @rpc(Unicode,_returns=sms_loc_info)
    def getHallByID(self,hallid):
        if hallid in g_sms_location.keys():
            return g_sms_location[hallid]
        else:
            return sms_loc_info()

    @rpc(_returns=Array(sms_stat))
    def getHallsState(self):
        a = []
        key_ls = g_sms_stat.keys()
        key_ls.sort()
        for i in key_ls:
            a.append( g_sms_stat[i])
        return a

    @rpc(Unicode,_returns=Integer)
    def getHallStateByID(self,hallid):
        if hallid in g_sms_stat.keys():
            return g_sms_stat[hallid]
        else:
            return 0

    @rpc(Unicode,Unicode,_returns=Integer)
    def setHallRunHost(self,hallid,host):
        #print u'setHallRunHost %s %s'%(hallid,host)
        agent_mgr = getMainObj()
        if agent_mgr:
            all_run_sms = agent_mgr.get_all_run_sms()
            cur_host = all_run_sms[hallid][2]
            #print 'setHallRunHost switch %s %s %s'%(hallid,cur_host,host)
            if cur_host != host:
                agent_mgr.switch(hallid,cur_host,host)
                return 0
            else:
                return 1


    # return 0 :健康  1：节点有问题  2：数据库不同步 3：对端ping不通
    @rpc(_returns=Integer)
    def getNodeHealthStat(self):
        agent_mgr = getMainObj()
        is_health = agent_mgr.getNodeHealthStat()
        db_stat = getDBSyncStat()
        hdfs_stat = getHDFSStat()
        if hdfs_stat > 0:
            is_health = False
        if is_health and db_stat == 0:#都正常的情况
            return 0
        elif not is_health and db_stat == 0:#节点关闭或损坏和数据库正常的情况
            return 1
        elif is_health and db_stat == 1:#节点正常和数据库异常的情�?
            return 2
        elif is_health and db_stat == 2:#节点正常和对端ping不通的情况
            return 3
        else:
            return 1


    @rpc(Unicode,_returns=Integer)
    def killHall(self,hallid):
        return 0



    @rpc(_returns=Array(disk_stat))
    def getDiskStat(self):
        a =[]
        for i in g_disk_stat:
            a.append(g_disk_stat[i])
        return a

    @rpc(_returns=Array(eth_stat))
    def getEthStat(self):
        a = []
        for i in g_eth_stat:
            a.append(g_eth_stat[i])
        return a

    @rpc(_returns=Integer)
    def getMysqlStat(self):
        return 0



class mt_wsgi(SocketServer.ThreadingMixIn, simple_server.WSGIServer):
    pass

def webserverFun():
    soap_app = Application([IMonitorWebServices],
                           'IMonitorServices',
                           in_protocol=Soap11(validator="lxml"),
                           out_protocol=Soap11())
    wsgi_app = WsgiApplication(soap_app)

    # svr = simple_server.make_server(ip,port,wsgi_app)
    svr = mt_wsgi((ip, port), simple_server.WSGIRequestHandler)
    svr.set_app(wsgi_app)
    try:
        svr.serve_forever()
    finally:
        svr.server_close()



if __name__ == '__main__':

    webserverFun()
