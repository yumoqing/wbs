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

from appPublic.folderUtils import _mkdir

class MyLog:
    def __init__(self,logfile):
        _mkdir(os.path.dirname(logfile))
        self.logf = open(logfile,'a')
        self._stdout = False

    def stdout(self,flag=False):
        if flag:
            self._stdout = True
        else:
            self._stdout = False

    def __call__(self,*args,**argkv):
        dt = datetime.now()
        outstr = "[%04d-%02d-%02d %02d:%02d:%02d]" % (dt.year,dt.month,dt.day,dt.hour,dt.minute,dt.second)
        for a in args:
            outstr = outstr + str(a) + ' '
        for k,v in argkv.items():
            outstr = outstr + "%s=%s " % (k,str(v))
        outstr = outstr + '\n'
        self.logf.write(outstr)
        self.logf.flush()
        if self._stdout:
            print outstr

