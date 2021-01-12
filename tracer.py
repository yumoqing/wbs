# Internet Batch Schedule.py
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
import time
import subprocess as sp

import odbcutils.odbc as db

from appPublic.genetic import Genetic
from appPublic.folderUtils import _mkdir
from appPublic.exceldata import ExcelData
"""
cancel value explain:
1 canceled
0 normal

status value explain:
2 running
0 executed finish successsful
1 executed finish with error
"""

class OutsideBatchLog:
    def __init__(self,config,log,batchdate,sysid=1):
        self.log = log
        self.newlogcmd = config.outtracers.newlog
        self.updatelogcmd = config.outtracers.updatelog
        self.isexecutecmd = config.outtracers.isexecuted
        self.cancelcmd = config.outtracers.cancel
        self.sysid = sysid
        self.env = config.env
        self.batchdate = batchdate
    
    def newLog(self,name,sysgroup):
        cmds = self.newlogcmd + [self.batchdate,name,sysgroup,str(self.sysid)]
        p = sp.Popen(cmds)
        r = p.wait()
        self.log(cmds,"executed return ",r)
    
    def updateLog(self,name,sysgroup,status):
        cmds = self.newlogcmd + [self.batchdate,name,sysgroup,str(self.sysid),str(status)]
        p = sp.Popen(cmds)
        r = p.wait()
        self.log(cmds,"executed return ",r)
    
    def isExecuted(self,name,sysgroup,etldate=None,sysid=None):
        if etldate is None:
            etldate = self.batchdate
        if sysid is None:
            sysid = self.sysid
            
        cmds = self.isexecutecmd + [etldate,name,sysgroup,str(sysid)]
        p = sp.Popen(cmds)
        r = p.wait()
        self.log(cmds,"executed return ",r)
    
    def cancel(self):
        cmds = self.cancelcmd + [self.batchdate,str(self.sysid)]
        p = sp.Popen(cmds)
        r = p.wait()
        self.log(cmds,"executed return ",r)
            
class BatchLog:
    def __init__(self,config,batchdate,sysid=1):
        self.config = config
        self.sysid = sysid
        self.batchdate=batchdate
        self.conn = db.opendb(config.dsn)
    
    def newLog(self,name,sysgroup):
        sqlStr = """insert into batchstatus (id,batchdate,name,sysgroup,sysid,starttime,endtime,status,cancel) 
                    values ((%s) + 1,?,?,?,?,?,?,?,?)""" % self.config.selectmaxidSQL
        args = [self.batchdate,name,sysgroup,self.sysid,datetime.now(),None,2,0]
        try:
	        db.execSQL(self.conn,sqlStr,args=args)
        except Exception,e:
            print sqlStr,e
            raise e

    def updateLog(self,name,sysgroup,status):
        updateSQL="update batchstatus set status=?,endtime=? where name=? and sysgroup=? and status=2"
        db.execSQL(self.conn,updateSQL,args=[status,datetime.now(),name,sysgroup])

    def isExecuted(self,name,sysgroup,etldate=None,sysid=None):
        if etldate is None:
            etldate = self.batchdate
        if sysid is None:
            sysid = self.sysid
        querySQL="select * from batchstatus where name=? and sysgroup=? and batchdate=? and sysid=? and status=0 and cancel=0"
        recs = db.execSQL(self.conn,querySQL,args=[name,sysgroup,etldate,sysid],dataReturn=True)
        if recs is None or recs == []:
            return False
        return True

    def cancel(self):
        updateSQL="update batchstatus set cancel=1 where sysid=? and batchdate=? and cancel=0"
        db.execSQL(self.conn,updateSQL,args=[self.sysid,self.batchdate])

