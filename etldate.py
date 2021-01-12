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
from multiprocessing import Process
from optparse import OptionParser
import time
import dbmodel as db
#from appPublic.Config import getConfig
import xlrd
import threading

from appPublic.genetic import Genetic
from appPublic.folderUtils import _mkdir
from appPublic.exceldata import ExcelData

class ETLDate:
    def __init__(self,yyyymmdd):
        self.strdate = yyyymmdd
        try:
            self.y = int(yyyymmdd[:4])
            self.m = int(yyyymmdd[4:6])
            self.d = int(yyyymmdd[6:8])
        except:
            raise DateException("Invalid date string",yyyymmdd)
        try:
            self.date = datetime(self.y,self.m,self.d)
        except:
            raise DateException("Invalid date string",yyyymmdd)
        nd = self.date + timedelta(1)
        self.matches = ['D',yyyymmdd,yyyymmdd[4:]]
        if self.date.month != nd.month:
            self.matches.append('M00')
        # Monday = 0 ,...
        self.matches.append('W%d' % ((self.date.weekday()+1) % 7))
        self.matches.append('M%02d' % (self.date.day))
    
    def isTodayJob(self,jobPattern):
        jps = jobPattern.split(',')
        for i in jps:
            if i in self.matches:
                return True
        return False
        
