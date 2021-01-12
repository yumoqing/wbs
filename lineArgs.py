# Internet Batch Schedule.py
##################################
# Author
#   Yu Moqing, Longtop Ltd,Co.
#   yumoqing at gmail.com
# Copyrights(c)
#   copyrights(c) 2009.06-2009.07,All rights reserved
##################################

def lineArgs(cmd):
    l = [ c for c in cmd if c not in ['\r','\n'] ]
    cmd = ''.join(l)
    cmd = ' '.join(cmd.split('\t'))
    a = cmd.split(' ')
    b = [ i for i in a if a != '']
    return b
