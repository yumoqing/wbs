# Internet Batch Schedule
##################################
# Author
#   Yu Moqing, Longtop Ltd,Co.
#   yumoqing at gmail.com
# Copyrights(c)
#   copyrights(c) 2009.06-2009.07,All rights reserved
##################################

import os
import sys
from datetime import datetime,timedelta
from multiprocessing import Process
from optparse import OptionParser
import time
import dbmodel as db
#from appPublic.Config import getConfig
import xlrd
import threading
import subprocess as sp

from appPublic.genetic import Genetic
from appPublic.folderUtils import _mkdir
from appPublic.exceldata import ExcelData

from lineArgs import lineArgs

class ProgramRunner(Genetic,threading.Thread):
    def __init__(self,sysgroup,ct,cmds):
        threading.Thread.__init__(self)
        Genetic.__init__(self)
        self.ct = ct.lower()
        self.sysgroup = sysgroup
        self.cmds = cmds
        self.name = os.path.basename(cmds[0])
        self.rc = None
    
    def setupEnv(self):
        if self.ct in self.config.runnerpaths.keys():
            self.cmds[0] = os.path.join(self.config.workdir,self.config.runnerpaths[self.ct],self.cmds[0])

        if self.ct in self.config.runners.keys():
            self.cmds = self.config.runners[self.ct] + self.cmds

        env = {}
        for k,v in os.environ.items():
            env[k] = v
        env.update(self.config.env.__dict__)

        env['ETLDATE'] = self.etldate.strdate
        self.env = env
        #os.putenv('PATH',env['PATH'])
                
    def run(self):
        self._run()
    
    def finish(self):
        self.log(self.name,"finished(%d) ..." % self.rc)
        self.tracer.updateLog(self.name.upper(),self.sysgroup.upper(),self.rc)
        if self.rc != 0:
            self.log(self.name,"failed",self.parent.name)
            self.parent.stopMe()
    
    def begin(self):
        self.log(self.name,"begin ...")
        self.tracer.newLog(self.name.upper(),self.sysgroup.upper())
        
    def _run(self):
        self.begin()
        self.setupEnv()

        sl = self.parent.schedulelist()
        ss = sl + [self.name]
        _mkdir(os.path.join(self.config.logpath,self.etldate.strdate,*ss))
        
        ss = sl + [self.name,'stdout.txt']
        soutfp = os.path.join(self.config.logpath,self.etldate.strdate,*ss)
        soutf = open(soutfp,'w')
        
        ss = sl + [self.name,'stderr.txt']
        serrfp = os.path.join(self.config.logpath,self.etldate.strdate,*ss)
        serrf = open(serrfp,'w')

        self.log("cmds=",self.cmds)
        self.p = sp.Popen(self.cmds,stdout=soutf,stderr=serrf,shell=True,env=self.env)
        rc = self.p.wait()
        soutf.close()
        serrf.close()
        self.rc = rc

        self.finish()
        return rc

    def doit(self,runthread=False):
        self.start()
            
class Scheduleer(Genetic,threading.Thread):
    def __init__(self,parent,schfile,etldate,concurrency=1):
        threading.Thread.__init__(self)
        Genetic.__init__(self)
        self.parent = parent
        self.sysgroup = 'SCHEDULE'
        self.schfile = schfile
        self.name = os.path.basename(schfile)
        self.path = os.path.dirname(schfile)
        self.etldate = etldate
        self.concurrency = concurrency
        self.todayJobs = []
        self.threads=[]
        self.stop = False
    
    def stopMe(self):
        self.log(self.name,"stopMe() ....")
        self.stop = True
        self.rc = 1
        if self.parent is not None:
            self.parent.stopMe()
        self.log(self.name,"stoped")
        
    def schedulelist(self):
        sf = os.path.basename(self.schfile)
        print sf
        if self.parent is not None:
            return self.parent.schedulelist() + [sf]
        return [sf]
        
    def waitingExecuteCondition(self,cnt):
        if self.concurrency < 2:
            self.log(self.name,self.concurrency,"return here")
            return
        self.log(self.name,len(self.threads)," running thread",cnt, "limited")
        while len(self.threads) >= cnt:
            # self.log('test....',self.threads)
            dead = [ x for x in self.threads if not x.isAlive() ]
            # dd = [ d.log(d.name,"finished...") for d in dead ]
            a = [x for x in self.threads if not x in dead ]
            self.log("running programs ",len(a))
            # dd = [ d.log(d.name,"aliving...") for d in a ]
            self.threads = a
            time.sleep(0.1)
            
    def finish(self):
        self.log(self.name,"finished.. %d" % self.rc)
        self.tracer.updateLog(self.name.upper(),self.sysgroup.upper(),self.rc)
    
    def begin(self):
        self.log(self.name,"begin...")
        self.tracer.newLog(self.name.upper(),self.sysgroup.upper())
        
    def run(self,):
        self._run()
    
    def getScheduleItems(self):
        self.sitems = []
    
    def lineParser(self,line):
        return None

    def isExecuted(self,line):
        return False
                        
    def _run(self):
        self.begin()
        self.getScheduleItems()
        self.rc = 0
        for i in self.sitems:
            if self.stop:
                self.log(self.name,"stop anormally")
                break
            if self.isExecuted(i):
                self.log(i)
                continue
            self.waitingExecuteCondition(self.concurrency)
            obj = self.lineParser(i)
            if obj is None:
                continue
            obj.setParent(self)
            b = False
            if self.concurrency > 1:
                self.threads.append(obj)
                b = True
            self.log(self.name,"running in concurrency mode",b)
            obj.doit(runthread=b)
        self.waitingExecuteCondition(1)
        self.finish()
               
    def doit(self,runthread=False):
        if runthread:
            self.start()
        else:
            self._run()

class SchedulerM(Scheduleer):
    def getScheduleItems(self):
        self.sitems = []
        try:
            f = open(self.schfile,'r')
        except Exception,e:
            self.log(self.schfile,"can not open")
            return
        line=f.readline()
        while line != '' :
            line = line.split('#',1)[0]
            l = [ c for c in line if c not in ['\r','\n'] ]
            line = ''.join(l)
            if line == '':
                line = f.readline()
                continue
            fields = lineArgs(line)
            self.sitems.append(fields)
            line = f.readline()
        f.close()
    
    def lineParser(self,fields):
        if fields[0] == 'M':
            schfile = os.path.join(self.path,fields[1])
            concurrency = int(fields[2])
            ms = SchedulerM(self,schfile,self.etldate,concurrency)
            return ms
        elif fields[0] == 'P':
            schfile = os.path.join(self.path,fields[1])
            concurrency = int(fields[2])
            ps = ScheduleerP(self,schfile,self.etldate,concurrency)
            return ps
        else:
            self.log(schedule_type=fields[0],error_type="wrong schedule type",data=fields)
            return None
    
    def isExecuted(self,fields):
        name = os.path.basename(fields[1])
        r = self.tracer.isExecuted(name.upper(),self.sysgroup.upper())
        if r:
            self.log(name,self.sysgroup,"is executed")
        return r

        
class ScheduleerP(Scheduleer):
    def getScheduleItems(self):
        self.sitems = []
        try:
            f = open(self.schfile,'r')
        except Exception,e:
            raise ScheduleException(self.schfile,"can not open schedule file",self.schfile)
        line = f.readline()
        while line != '':
            line = line.split('#',1)[0]
            l = [ c for c in line if c not in ['\r','\n'] ]
            line = ''.join(l)
            fields = lineArgs(line)
            if len(fields)<5:
                line = f.readline()
                continue
            if self.etldate.isTodayJob(fields[0]):
                self.sitems.append(fields)
            line = f.readline()
        f.close()
    
    def lineParser(self,fields):
        self.log(fields=fields)
        if fields[3].lower() == 'wait':
            self.waiting(fields[4:])
            return None

        pr = ProgramRunner(fields[2],fields[3],fields[4:])
        pr.parent = self

        return pr
        
    def isExecuted(self,fields):
        name = os.path.basename(fields[4])
        r = self.tracer.isExecuted(name.upper(),fields[2].upper())
        if r:
            self.log(name,fields[2],"is executed")
        return r

    def waiting(self,args):
        for a in args:
            sysid,sysgroup,name=a.split(',')
            if sysid=='':
                sysid = self.config.sysid
            while not self.tracer.isExecuted(name,susgroup,sysid=sysid):
                time.sleep(1)
                
