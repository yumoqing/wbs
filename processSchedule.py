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

class DateException(Exception):
    def __init__(self,msg,datestr):
        self.msg = msg
        self.datestr = datestr
        Exception.__init__(self)
    
    def __str__(self):
        return "<DateException %s:%s>" % (self.msg,self.datestr)

class ScheduleException(Exception):
    def __init__(self,schfile,msg,info):
        self.schfile = schfile
        self.msg = msg
        self.info = info
        Exception.__init__(self)
    
    def __str__(self):
        return "<ModuleScheduleException file(%s) %s:%s>" % (self.schfile,self.msg,self.info)


class ModuleSchedule(Genetic):
    def __init__(self,schfile,etldate):
        self.schfile = schfile
        self.path=os.path.dirname(schfile)
        self.etldate = etldate
        
    def run(self):
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
            if fields[0] == 'M':
                schfile = os.path.join(self.path,fields[1])
                ms = ModuleSchedule(schfile,self.etldate)
                ms.setParent(self)
                ms.run()
            elif fields[0] == 'P':
                schfile = os.path.join(self.path,fields[1])
                concurrency = int(fields[2])
                ps = ProgramSchedule(schfile,self.etldate,concurrency)
                ps.setParent(self)
                ps.run()
            else:
                self.log(schedule_type=fields[0],error_type="wrong schedule type",data=fields)
                
            line = f.readline()
        f.close()
        
class ConcurrencyProcessController(Genetic):
    def __init__(self,etldate,concurrencyCount,cmds):
        self.concurrency = concurrencyCount
        self.cmds = cmds
        self.etldate = etldate
        self.cur = 0
        self.procs = []
            
    def _runner(self,etldate,*args,**kwargs):
        progname = args[1]
        args = [ a for a in args ]
        ct = args[0].lower()
        if ct in self.config['runnerpaths'].keys():
            args[1] = self.config['runnerpaths'][ct] + os.sep + args[1]

        program = args[1]
        if ct in self.config['runners'].keys():
            a = [i for i in args[1:]]
            args = self.config['runners'][ct] + a
        else:
            args = args[1:]
        if sys.platform.lower() == 'win32':
            envpathsep = ';'
        else:
            envpathsep = ':'
        
        env = {}
        for k,v in os.environ.items():
            env[k] = v
        for k,v in self.config['env'].items():
            if type(v) == type(''):
                env[k] = v

        rpmu5117=os.path.abspath(self.config['env']['PROGRAMPATH'])
        path = os.environ['PATH']
        path += envpathsep + rpmu5117
        env['PATH'] = path
        env['ETLDATE'] = etldate

        path = os.path.join(self.config['logpath'],etldate,progname)
        soutf = os.path.join(path,'stdout')
        serrf = os.path.join(path,'stderr')
        stdout = open(soutf,'w')
        stderr = open(serrf,'w')
        os.close(1)
        if os.dup(stdout.fileno()) != 1:
            sys.stderr.write("bad write dup")
        os.close(2)
        os.dup(stderr.fileno())
        try:
            os.execvpe(args[0],args,env)
        except Exception,e:
            self.log(args[0],"executed error",cmdline=' '.join(args))
            sys.exit(1)
            
    def runner(self,cmd):
        if self.tracer.isExecuted(cmd[4],cmd[2]):
            self.log(name=cmd[4],sysgroup=cmd[2],status="executed before")
            return
            
        self.log(program=cmd[4],type=cmd[2],status="running...")
        if cmd[1] == 'Y' or cmd[1] == 'y':
            self.tracer.newLog(cmd[4],cmd[2])
        path = os.path.join(self.config['logpath'],self.etldate.strdate,cmd[4])
        _mkdir(path)
        p = Process(target=self._runner,args=[self.etldate.strdate] + cmd[3:])
        p.start()
        p.runcmd = cmd
        if sys.platform == 'win32':
            time.sleep(1)
        return p
        
    def run(self):
        for cmd in self.cmds:
            if self.tracer.isExecuted(cmd[4],cmd[2]):
                self.log(name=cmd[4],sysgroup=cmd[2],status="executed before")
                continue
            self.checkAvailable(self.concurrency)
            p = self.runner(cmd)
            self.procs.append(p)
            self.cur += 1
        self.checkAvailable(1)
    
    def checkAvailable(self,cnt=1):
        i = cnt
        print self.procs
        while i >= cnt:
            time.sleep(0.01)
            dead = [p for p in self.procs if not p.is_alive() ]
            procs = [ p for p in self.procs if p not in dead ]
            i = len(procs)
            self.procs = procs
            self.cur =  i
            for p in dead:
                self.tracer.updateLog(p.runcmd[4],p.runcmd[2],p.exitcode)
                self.log(p.runcmd[4],p.runcmd[2],'finish',p.exitcode)

class ProgramSchedule(Genetic):
    def __init__(self,schfile,etldate,concurrency):
        self.schfile = schfile
        self.path = os.path.dirname(schfile)
        self.etldate = etldate
        self.concurrency = concurrency
        self.todayJobs = []
    
    def getTodayJobs(self):
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
                self.todayJobs.append(fields)
            line = f.readline()
        f.close()
        
    def run(self):
        try:
            self.getTodayJobs()
        except Exception,e:
            print e
            return
        cpc = ConcurrencyProcessController(self.etldate,self.concurrency,self.todayJobs)
        cpc.setParent(self)
        cpc.run()
                
def setCurrentDir(f):
    f = os.path.abspath(f)
    if not os.path.exists(f):
        print f," not exists"
        sys.exit(1)
    os.chdir(os.path.dirname(f))

class AppSystem:
    def __init__(self,sysid):
        self.sysid = sysid
        self.bl = BatchLog(self.sysid)

    def newBatchLog(self,etldate):
        self.bl = BatchLog(etldate,self.sysid)
