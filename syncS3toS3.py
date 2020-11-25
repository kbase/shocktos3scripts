#!/usr/bin/python

'''
This is a proof of concept modifying our existing synctool.py for Shock sync to do
S3 to S3 sync instead.

To do:
  * support full date/time instead of just day, to avoid recopying files (in case script is run multiple times/day)
    * use datetime.fromisoformat
  * use configparser instead of configobj
  * configure source and destination as separate sections in config file
  * better documentation of files used
'''

import os
import sys
from pymongo.mongo_client import MongoClient
import datetime
from multiprocessing import Pool
from subprocess import call
from pprint import pprint
import configparser
import argparse

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
  

def readdatefile(filename):
  fo=open(filename,"r")
  date=fo.readline()
  fo.close()
  return date.replace('\n','').split('-')
  

def writedatefile(filename,date):
  fo=open(filename,"w")
  fo.write("%s\n"%(date))
  fo.close()

def getObjects(start):
  if conf['main']['mongo_user']:
    client = MongoClient(conf['main']['mongo_host'], authSource=conf['main']['mongo_database'],
      username=conf['main']['mongo_user'], password=conf['main']['mongo_pwd'], retryWrites=False)
  else:
    client = MongoClient(conf['main']['mongo_host'])
  db = client.conf['main']['mongo_database']

  ct=0
  ids=[]
  # to do: use ObjectID for ws. maybe for all queries?  or use date field for blobstore queries?
  for object in db.conf['main']['mongo_collection'].find({ 'created_on': {'$gt': start}}, {'id': 1, '_id': 0}):
    ids.append(object['id']) #.split(': u\'')[1].replace("'}\n",''))
    ct+=1
  return ids

def syncnode(id):
  if id in done:
    #writelog(conf['logfile'],id)
    return 0
  # to do: both ws and blobstore collections should have full S3 paths already
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
  comm=("echo","mc","cp",spath,dpath)
  result=call(comm)
  if result==0:
    writelog(conf['logfile'],id)
  else:
    writelog(conf['retryfile'],id)
   
  return result 

if __name__ == '__main__':
  parser = argparse.ArgumentParser(description='Copy object list from a MongoDB collection from one S3 store to another.')
  parser.add_argument('--config-file', dest='configfile', required=True,
		    help='Path to config file (INI format). (required)')
  args = parser.parse_args()

  configfile=args.configfile
  conf=configparser.ConfigParser()
  conf.read(configfile)
  
  if os.path.exists(conf['main']['datefile']):
    startString=readdatefile(conf['main']['datefile'])
  else:
    print "Warning: no datefile.  Using now."
    startString=datetime.datetime.now().isoformat()
# datetime.datetime.strptime("2007-03-04T21:08:12Z", "%Y-%m-%dT%H:%M:%SZ")
  start = datetime.datetime.strptime(startString,"%Y-%m-%dT%H:%M:%S.%f")
  readlog(conf['main']['logfile'],done)
  readlog(conf['main']['retryfile'],retry)
  mystart=datetime.date.today()
  if int(conf['main']['debug'])==1:
    debug=1

  if debug:
    print "querying mongo"
  objectList=getObjects(start)
  # TODO append retry to objectList
  for item in retry:
    objectList.append(item)
  print >> sys.stderr, 'ct=%d'%(len(objectList))
  if int(conf['main']['resetlog'])==1:
    initlog(conf['main']['logfile'])  

  initlog(conf['main']['retryfile'])

  print >> sys.stderr, 'start=%s'%(start)

  pool = Pool(processes=int(conf['main']['nthreads']))
  results=pool.map(syncnode, objectList)
  failed=0
  for result in results:
    if result:
      failed+=1
  print "failed: %d"%(failed)
    
  writedatefile(conf['main']['datefile'],mystart)
  
