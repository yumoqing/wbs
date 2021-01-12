#!/usr/bin/python
import os
import sys
import re
import tempfile
import subprocess as sp
"""
transfer string with environment variable values
"""

def replace(s,var,value):
    l = s.split('%'+var+'%')
    return str(value).join(l)

def envString(s):
    env = os.environ
    for k in env.keys():
    	env[k.upper()] = env[k]
    keys = env.keys()
    #print keys
    vl = re.findall("%(\w+)%",s,re.MULTILINE)
    #print vl
    for v in vl:
        if v in keys:
            s = replace(s,v,env[v])
    return s

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print "Usage %s cmd arg1 arg2 ..." % sys.argv[0]
        sys.exit(1)
        
    args = []
    args.append(sys.argv[1])
    for arg in sys.argv[2:]:
        if os.path.isfile(arg):
            outf = tempfile.mktemp()
            f = open(arg,'rb')
            data = f.read()
            f.close()
            f = open(outf,"wb")
            f.write(envString(data))
            f.close()
            args.append(outf)
        else:
            args.append(envString(arg))
            
    p = sp.Popen(args)
    rc = p.wait()
    sys.exit(rc)