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
import sys
import copy
import pdb



class StatMgr:
    '''
    状态管理：
    1、状态转换
    2、接管管理
    3、恢复管理

    '''
    def __init__(self,agent_ls,locationmgr,msg_queue,log):
        self.LM = locationmgr
        self.takeover_matrx = {}
        self.stat_matrix = {}
        self.msg_queue = msg_queue
        for agent_name in agent_ls:
          self.stat_matrix[agent_name] = {'agent_state':['unregist','unregist'],'take_over':{},'sms':{},'enable':False}
        self.cluster_stat = ['booting','booting']
        self.stat_lock = threading.Lock()
        self.log = log


    # 设置集群状态
    def set_cluster_stat(self,stat):
        self.stat_lock.acquire()
        self.cluster_stat[0] = self.cluster_stat[1]
        self.cluster_stat[1] = stat
        self.stat_lock.release()

    # 获取集群状态
    def get_cluster_stat(self):
        stat = self.cluster_stat[1]
        return stat



    def update_agent_state(self,agent_name,state,value=''):
        '''
        agent state:unregist regist/regist_outtime/takeovered spawning watching switching
        sms   state:unregist stat
        '''
        log =  'prevous agent(%s) stat:(%s)\n'%(agent_name,self.stat_matrix[agent_name])
        log +=  "update_agent_state:agent:%s state:%s value=%s"%(agent_name,state,value)
        self.log.info(log)
        new_stat = self.stat_matrix[agent_name]['agent_state']
        new_stat[0] = new_stat[1]            # 保留上次的状态
        new_stat[1] = state                  # 保存新的状态

        if (new_stat[0] in ['regist','unregist','spawning','watching'])\
                and new_stat[1] == 'regist':     # 首次注册成功
            log = 'enter spawn process'
            self.log.info(log)
            self.LM.enable_agent(agent_name)
            self.stat_matrix[agent_name]['enable'] = True
            run_sms = self.LM.get_agent_runsms(agent_name,0)          # 获取在agnet_name上第一路径运行的sms
            run_sms_txt = json.dumps(run_sms)                         # 转成json
            msg_txt = '{"%s":["spawn",%s]}'%(agent_name,run_sms_txt)
            self.msg_queue.put(msg_txt)
        if new_stat[1] == 'regist_timeout':
            #找到接管此主机的主机，并发送消息给上层
            switch_agent = {}
            switch_ls = []
            self.LM.disable_agent(agent_name)
            self.stat_matrix[agent_name]['enable'] = False
            log = 'enter take over process'
            self.log.info(log)
            # 获取主机当前运行的sms
            #if new_stat[0] in ['watching','regist_timeout','regist']:
            sms_dict = self.stat_matrix[agent_name]['sms']
            log = 'find %s run sms'%agent_name,sms_dict
            self.log.info(log)
            takeover_dict = self.stat_matrix[agent_name]['take_over']

            log = 'find %s takeover sms'%agent_name,takeover_dict
            self.log.info(log)
            for sms in sms_dict:
                #if sms_dict[sms] == 1 :#RUN:
                switch_ls.append(sms)
            switch_ls += takeover_dict.keys()

            # 获取这台主机运行的sms,查找第一路径在这台主机上运行的sms,
	    # 再获取这些sms的第二或第三路径直到找到运行正常的主机，
            if new_stat[0] == 'unregist':
                sms_runls = self.LM.get_agent_runsms(agent_name,0,False)
                for sms_id in sms_runls:
                    if sms_runls[sms_id] == 1:#RUN
                        switch_ls.append(sms_id)
                log =  'prevous status is unregist ,so get %s run sms',switch_ls
                self.log.info(log)
            self.takeover_matrx['takedover_%s'%agent_name] = switch_ls

            # 获取sms可以运行的主机
            log = 'sms which prepare takover is ',switch_ls
            self.log.info(log)
            for sms in switch_ls:
                agent_id = self.find_run_host(sms)
                if agent_id not in switch_agent.keys():
                    switch_agent[agent_id] = {}
                switch_agent[agent_id][sms] = 1 #RUN

            # 按主机发送接管消息
            for agent in switch_agent.keys():
                msg_txt = '{"%s":["take_over",%s]}' % (agent, switch_agent[agent])
                self.msg_queue.put(msg_txt)
            
            # 把接管的sms在被接管的主机上关闭
            if 'spawn' in self.stat_matrix[agent_name].keys() :
               stop_dict = self.stat_matrix[agent_name]['spawn']
               for sms_id in stop_dict:
                   stop_dict[sms_id] = 8 #stop
               stop_sms_txt = json.dumps(stop_dict)
               msg_txt = '{"%s":["spawn",%s]}'%(agent_name,stop_sms_txt)
               self.msg_queue.put(msg_txt)

        if new_stat[0] == 'regist_timeout' and new_stat[1] == 'regist':
            # 恢复接管,找到本机被其它主机接管的厅号
            log = 'enter restore takeover process'
            self.log.info(log)
            sms_ls = self.takeover_matrx['takedover_%s' % agent_name]

            # 找到每个被接管厅运行的位置
            sms_loc = self.get_takeover_sms_in_all_agent()
            switch_ls = {}
            for sms_id in sms_ls:
                if sms_loc and sms_id in sms_loc:
                    switch_ls[sms_id] = sms_loc[sms_id]

            msg_txt = '{"%s":["backing_takeover",%s]}' % (agent_name, switch_ls)
            self.msg_queue.put(msg_txt)


        if new_stat[1] == 'shutdown_sms' :
            log = 'enter shutdown sms process'
            self.log.info(log)
            self.stat_matrix[agent_name]['shutdown_sms'] = value

        elif new_stat[1] == 'startup_sms' :
            log = 'enter startup sms process'
            self.log.info(log)
            self.stat_matrix[agent_name]['startup_sms'] = value



        elif new_stat[1] == 'spawning':
            #记录spawn的sms
            if value :
                log =  '%s::get msg spawning '%(sys._getframe().f_code.co_name),value
                self.log.info(log)
                spawn_sms_dict = json.loads(value)
                self.stat_matrix[agent_name]['spawn'] = spawn_sms_dict['spawn']

        elif new_stat[1] == 'take_over':
            #记录takeover的sms
            if value:
                value = value.replace('u', '').replace("'", '"')
                log =  'take_over:get msg ', value
                self.log.info(log)
                takeover_sms_dict = json.loads(value)
                self.stat_matrix[agent_name]['take_over'] = takeover_sms_dict['take_over']

        elif new_stat[1] == 'shutdown_done':
            if value:
                spawn_dic = self.stat_matrix[agent_name]['spawn']
                if value in spawn_dic.keys():
                    spawn_dic[value] = 8 #STOP

        elif new_stat[1] == 'startup_sms_done':
            if value:
                spawn_dic = self.stat_matrix[agent_name]['spawn']
                if value in spawn_dic.keys():
                    spawn_dic[value] = 1  # STOP

        elif new_stat[1] == 'take_over_done':
            log = "agnet %s take over done " % agent_name
            self.log.info(log)

	    


        log = 'agent(%s) cur stat:'%agent_name, self.stat_matrix[agent_name]
        self.log.info(log)


    def update_sms_state(self,agent_name,sms_name,state):
        log = 'update_sms_state:%s@%s state:'%(agent_name,sms_name),state
        self.log.info(log)
        #print 'prevous agent(%s) stat:'%agent_name, self.stat_matrix[agent_name]
        stat_dict = self.stat_matrix[agent_name]['sms']
        NEW = 1
        OLD = 0
        if sms_name in stat_dict.keys():
            new_stat = stat_dict[sms_name]
            new_stat[0] = new_stat[1]      # 保留上次的状态
            if state == 'delete':
                new_stat[NEW] = '0'
                state = '0'
            else:
                new_stat[NEW] = state              # 保存新的状态
        else:
            stat_dict = self.stat_matrix[agent_name]['sms']
            stat_dict[sms_name] = ['',state]

        if self.stat_matrix[agent_name]['agent_state'][1] == 'spawning'  and int(state) > 100:
            spawn_sms_dict =  self.stat_matrix[agent_name]['spawn']
            id_ls = []
            for id in spawn_sms_dict:
                if spawn_sms_dict[id] == 1:     #找到所有在本机启动的sms
                    id_ls.append(id)
            if  stat_dict.keys().sort() == id_ls.sort():      #先排序，否则可能出现也相等，所有spawn的sms都启动成功,则转到watching 状态
                self.update_agent_state(agent_name,'watching')

                #如果所有agent都进入watching状态，在把集群状态更新到working
                stat_matrix = self.get_stat_matrix()
                bFind = False
                for agent_id in stat_matrix.keys():
                    if stat_matrix[agent_id]['agent_state'][1] != 'watching':
                        bFind = True

                if not bFind:
                    self.set_cluster_stat('working')
        elif self.stat_matrix[agent_name]['agent_state'][1] == 'take_over'  and int(state) > 100:
            takeover_sms_dict =  self.stat_matrix[agent_name]['take_over']
            diff = set(takeover_sms_dict).difference(set(stat_dict))#求差集
            if not diff:#如果是空集，则说明takeover_sms_dict全部包含到stat_dict了
                self.update_agent_state(agent_name, 'watching')  # 所有take over的sms都启动成功
                self.update_agent_state(agent_name, 'take_over_done')  # 所有take over的sms都启动成功
                self.takeover_matrx[agent_name] = takeover_sms_dict.keys()

        elif self.stat_matrix[agent_name]['agent_state'][1] == 'startup_sms'  and int(state) > 100:
            start_sms = self.stat_matrix[agent_name]['startup_sms']
            if sms_name == start_sms:
                del self.stat_matrix[agent_name]['startup_sms']
                self.update_agent_state(agent_name, 'startup_sms_done', sms_name)  # 更新spawn的状态
                self.update_agent_state(agent_name, 'watching')  # 完成启动
                
                # 如果所有agent都进入watching状态，在把集群状态更新到working
                stat_matrix = self.get_stat_matrix()
                bFind = False
                for agent_id in stat_matrix.keys():
                    if stat_matrix[agent_id]['agent_state'][1] != 'watching':
                        bFind = True

                if not bFind:
                    self.set_cluster_stat('working')

        elif self.stat_matrix[agent_name]['agent_state'][1] == 'shutdown_sms' and int(state) == '0':# 等于0就是等于delete
            shutdown_sms = new_stat = self.stat_matrix[agent_name]['shutdown_sms']
            if sms_name == shutdown_sms:
                del self.stat_matrix[agent_name]['shutdown_sms']
                self.update_agent_state(agent_name, 'watching') #完成关闭
                self.update_agent_state(agent_name, 'shutdown_done',sms_name)  # 更新spawn的状态

                # 如果所有agent都进入watching状态，在把集群状态更新到working
                stat_matrix = self.get_stat_matrix()
                bFind = False
                for agent_id in stat_matrix.keys():
                    if stat_matrix[agent_id]['agent_state'][1] != 'watching':
                        bFind = True

                if not bFind:
                    self.set_cluster_stat('working')
                msg_txt = '{"%s":["switch_done","%s"]}'%(agent_name,sms_name)
                self.msg_queue.put(msg_txt)

        log = 'agent(%s) cur stat:'%agent_name,self.stat_matrix[agent_name]
        self.log.info(log)



    def get_stat_matrix(self):
        stat_matrix = copy.deepcopy(self.stat_matrix)
        return stat_matrix

    # 获取所有sms的名称
    def get_sms_name(self):
        return self.LM.get_sms_name()

    # 获取所有接管运行的sms的位置信息，返回形如{'sms1':'slave1'}的结果
    def get_takeover_sms_in_all_agent(self):
        sms_loc = {}
        for agent_id in self.stat_matrix:
            to_ls = self.stat_matrix[agent_id]['take_over']
            for sms_id in to_ls:
                if sms_id not in sms_loc:
                    sms_loc[sms_id] = agent_id
        return sms_loc



    # 获取运行在agent上的sms
    def get_run_sms_in_agent(self,agent_name):
        to_dict = self.stat_matrix[agent_name]['take_over']
        stat_dict = self.stat_matrix[agent_name]['sms']
        return stat_dict.keys()+to_dict.keys()

    # 获取agent对应的ip
    def get_agent_host_ip(self,agent_name):
        return self.LM.get_agent_host_ip(agent_name)

    # 获取所有运行的sms的位置，状态
    def get_all_run_sms_info(self):
        sms_info = {}
        for agent_name in self.stat_matrix.keys():
            stat_dict = self.stat_matrix[agent_name]['sms']
            ip = self.LM.get_agent_host_ip(agent_name)
            for smsid in stat_dict.keys():
                if int(stat_dict[smsid][1])>100:
                    sms_info[smsid] = [stat_dict[smsid][1],ip,agent_name]#取最新的状态
        return sms_info

    # 获取制定主机上的sms的最新状态
    def get_sms_stat(self,agent_name,sms_name):
        stat_dict = self.stat_matrix[agent_name]['sms']
        if sms_name in stat_dict.keys():
            return stat_dict[sms_name][1]
        else:
            return -1


    # 查找可运行sms的主机
    def find_run_host(self,sms_name):
        agent_num = len(self.LM.get_agent_name())
        for i in range(agent_num):
            agent_name = self.LM.get_sms_location(sms_name, i, True)
            if agent_name:
                return agent_name
    #获取sms的指定运行优先级下的主机
    def get_sms_run_host_in_piroirty(self,sms,pirority):
        return self.LM.get_sms_location(sms,pirority,True)

    # 获取不可用的主机
    def getDisableHost(self):
        return self.LM.get_disalbe_host()

    def set_stat_matrix(self,stat_matrix):
        self.stat_matrix = stat_matrix
        for agent_id in self.stat_matrix:
            if self.stat_matrix[agent_id]['enable'] :
                self.LM.enable_agent(agent_id)
                log = 'set %s enable True'%agent_id
                self.log.info(log)





