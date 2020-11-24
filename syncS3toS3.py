#!/usr/bin/python

```
This is a proof of concept modifying our existing synctool.py for Shock sync to do
S3 to S3 sync instead.

To do:
  * support full date/time instead of just day, to avoid recopying files (in case script is run multiple times/day)
  * use configparser instead of configobj
  * configure source and destination as separate sections in config file
```

import os
import sys
import pymongo
import datetime
from time import sleep
from multiprocessing import Pool
from subprocess import call
from configobj import ConfigObj

done=dict()
retry=dict()
debug=0

def initlog(filename):
  fo=open(filename,"w")
  fo.close()

def writelog(filename,node):
  fo=open(filename,"a")
  fo.write("%s\n"%(node))
  fo.close()

def readlog(filename,list):
  if not os.path.exists(filename):
    print "Warning: %s doesn't exist"%(filename)
    return
  with open(filename,"r") as f:
    for id in f:
      list[id.rstrip()]=1
    f.close()
  

def readcp(filename):
  fo=open(filename,"r")
  date=fo.readline()
  fo.close()
  return date.replace('\n','').split('-')
  

def writecp(filename,date):
  fo=open(filename,"w")
  fo.write("%s\n"%(date))
  fo.close()

def getnodes(start):
  c = pymongo.MongoClient(conf['mongo_host']);
  db=c.ShockDB
  db.authenticate( conf['mongo_user'],conf['mongo_pwd'])

  ct=0
  ids=[]
  for x in db.Nodes.find({ 'created_on': {'$gt': start}}, {'id': 1, '_id': 0}):
    ids.append(x['id']) #.split(': u\'')[1].replace("'}\n",''))
    ct+=1
  return ids

def syncnode(id):
  if id in done:
    #writelog(conf['logfile'],id)
    return 0
  spath="%s/%s/%s/%s/%s"%(conf['src'],id[0:2],id[2:4],id[4:6],id)
  dpath="%s/%s/%s/%s/"%(conf['dst'],id[0:2],id[2:4],id[4:6])
  if not os.path.isdir(spath):
    print "Src missing "+spath
    writelog(conf['retryfile'],id)
    return 1
    
  
  print "syncing %s"%(id)
  # example from vadmin1:
  # assumes `minio` and `prod-ws01` are defined endpoints in ~/.mc/config.json
  # /opt/mc/mc cp minio/prod-ws/00/00/00/000000e7-0d44-494b-bd17-638f2a904329 prod-ws01.gcp/prod-ws01/00/00/00/000000e7-0d44-494b-bd17-638f2a904329
  comm=("rsync","-aqz","--bwlimit=%d"%(int(conf['bw'])),spath,dpath)
  result=call(comm)
  if result==0:
    writelog(conf['logfile'],id)
  else:
    writelog(conf['retryfile'],id)
   
  return result 

  
def usage():
  print "Usage: synctool <config file>"


if __name__ == '__main__':
  if len(sys.argv)<2:
    usage()
    sys.exit(-1) 
  conffile=sys.argv[1]
  conf=ConfigObj(sys.argv[1])
  if 'bw' not in conf:
    usage()
    sys.exit(-1)
  if os.path.exists(conf['cpfile']):
    startl=readcp(conf['cpfile'])
  else:
    print "Warning: no cpfile.  Using today."
    startl=str(datetime.date.today()).split('-')
  start = datetime.datetime(int(startl[0]),int(startl[1]),int(startl[2]),0,0,0)
  readlog(conf['logfile'],done)
  readlog(conf['retryfile'],retry)
  mystart=datetime.date.today()
  if debug in conf and int(conf['debug'])==1:
    debug=1
  if debug:
    print "querying mongo"
  dolist=getnodes(start)
  # TODO append retry to dolist
  for item in retry:
    dolist.append(item)
  print >> sys.stderr, 'ct=%d'%(len(dolist))
  if int(conf['resetlog'])==1:
    initlog(conf['logfile'])  

  initlog(conf['retryfile'])  

  print >> sys.stderr, 'start=%s'%(start)

  pool = Pool(processes=int(conf['nthreads']))
  results=pool.map(syncnode, dolist)
  failed=0
  for result in results:
    if result:
      failed+=1
  print "failed: %d"%(failed)
    
  writecp(conf['cpfile'],mystart)
  
