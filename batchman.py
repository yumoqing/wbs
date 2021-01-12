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

from optparse import OptionParser
from appPublic.exceldata import ExcelData
from appPublic.Config import getConfig

import psycopg2
import dbmodel as db
from mylog import MyLog
from lineArgs import lineArgs
from etldate import ETLDate
from tracer import OutsideBatchLog,BatchLog
from threadSchedule import SchedulerM
from appPublic.folderUtils import ProgramPath


def setupRunners(config):
    for k in config.runners.keys():
        a = []
        for p in config.runners[k]:
            if p[0] == '.':
                p = os.path.join(config.programPath,p)
            a.append(p)
        config.runners[k] = a
    #print config.runners 
    
def configSetup(config):
    config.programPath = ProgramPath()
    setupRunners(config)
	
if __name__ == '__main__':
    usage="%s [option] ETLDATE" % sys.argv[0]
    parser = OptionParser(usage)
    parser.add_option("-i","--init",dest="init",action="store_true",default=False,
                            help="initial database",metavar="Boolean")
                            
    parser.add_option("-c","--config",dest="conf_file",default="conf/config.ini",
                            help="configure file path")

    parser.add_option("-s","--sysid",dest="sysid",default=1,
                            help="batch system id in integer value",metavar="int")

    parser.add_option("-r","--rerun",dest="rerun",action="store_true",default=False,
                            help="rerun all batch programs",metavar="Boolean")

    #parser.add_option("-p","--process",dest="runproc",action="store_true",default=False,
    #                        help="run schedule in multiprocess mode")
                            
    parser.add_option("-o","--outsidelog",dest="outsidelog",action="store_true",default=False,
                            help="use a outside tracer for batch status")
                            
    (options,args) = parser.parse_args()
 
    if len(args)<1 and not options.init:
        print "Usage:\n",sys.argv[0]," ETLDATE"
        print "type %s --help for HELP" % sys.argv[0]
        # print dir(options)
        
        sys.exit(1)
    
    #configFile = args[0]
    #ed = ExcelData(configFile)
    #ed.dict()['config']
    
    config = getConfig(options.conf_file)
    configSetup(config)
    # print 'config=',config,"conf_file=",options.conf_file
    ctSQL = getattr(config,'ctSQL',None)
    if options.init:
        db.dbinit(config.dsn,ctSQL)
        sys.exit(0)

    etldate = ETLDate(args[0])
    # db.dbopen(config['dburl'])
    config.logpath = os.path.join(config.workdir,'logs')
    path = os.path.join(config.logpath,etldate.strdate)
    try:
        _mkdir(config.logpath)
    except:
       pass

    logfile = os.path.join(path,config.logfile)
    log = MyLog(logfile)
    log.stdout(True)
    config.schedulepath = os.path.join(config.workdir,'schedules')
    rootf = os.path.join(config.schedulepath,config.rootschedule)
    if hasattr(config,'outtracer'):
        tracer = OutsideBatchLog(config,log,etldate.strdate,options.sysid)
    else:
        tracer = BatchLog(config,etldate.strdate,options.sysid)
        
    if options.rerun:
        tracer.cancel()

    m = SchedulerM(None,rootf,etldate)
    m.tracer = tracer
    m.log = log
    m.config = config
    m.doit()
        