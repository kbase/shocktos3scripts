#!/usr/bin/python

'''
This is a beta version modifying our existing synctool.py for Shock sync to do
S3 to S3 sync instead.

To do:
  * support ws/blobstore mode
  * check for target object and skip if exists (check MD5 too?)
  * add support for end date
  * better documentation of files used
'''

import os
import sys
from pymongo.mongo_client import MongoClient
import bson
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
  return date.replace('\n','')
  

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
  db = client[conf['main']['mongo_database']]
# pprint(db)

  ct=0
  ids=[]

# to do: use ObjectID for ws. maybe for all queries?  or use date field for blobstore queries?
#bson.ObjectId.from_datetime(CONFIG_START_DATE)
#    idQuery = {'_id': {'$gt': CONFIG_WS_OBJECTID_START, '$lt': CONFIG_WS_OBJECTID_END }}


  idQuery = {'_id': {'$gt': bson.ObjectId.from_datetime(start) } }
 
  for object in db[conf['main']['mongo_collection']].find(idQuery):
    id=object[conf['main']['mongo_keyfield']]
    if (conf['main']['mode'] == 'blobstore'):
        id=object[conf['main']['mongo_keyfield']].split(': u\'')[1].replace("'}\n",''))
#    pprint(object)
    ids.append(id)
    ct+=1
  return ids

def syncnode(id):
  if id in done:
    #writelog(conf['logfile'],id)
    return 0
  # to do: ws collection has full S3 paths already
  # blobstore does not so need to convert
  spath="%s/%s/%s"%(conf['source']['endpoint'],conf['source']['bucket'],id)
  dpath="%s/%s/%s"%(conf['destination']['endpoint'],conf['destination']['bucket'],id)
 
  print "syncing %s"%(id)
  # example from vadmin1:
  # assumes `minio` and `prod-ws01` are defined endpoints in ~/.mc/config.json
  # /opt/mc/mc cp minio/prod-ws/00/00/00/000000e7-0d44-494b-bd17-638f2a904329 prod-ws01.gcp/prod-ws01/00/00/00/000000e7-0d44-494b-bd17-638f2a904329
  comm=(conf['main']['mcpath'],'--quiet','cp',spath,dpath)
# use echo for testing
#  comm=('echo',conf['main']['mcpath'],'--quiet','cp',spath,dpath)
  if (conf['main'].getboolean('insecureminio') == True):
      comm=(conf['main']['mcpath'],'--quiet','--insecure','cp',spath,dpath)
      # use echo for testing
      comm=('echo',conf['main']['mcpath'],'--quiet','--insecure','cp',spath,dpath)
  result=call(comm)
  if result==0:
    writelog(conf['main']['logfile'],id)
  else:
    writelog(conf['main']['retryfile'],id)
   
  return result 

if __name__ == '__main__':
  parser = argparse.ArgumentParser(description='Copy object list from a MongoDB collection from one S3 store to another.')
  parser.add_argument('--config-file', dest='configfile', required=True,
		    help='Path to config file (INI format). (required)')
  parser.add_argument('--end-date', dest='enddate',
		    help='End date for query (optional, default now)')
  args = parser.parse_args()

  configfile=args.configfile
  conf=configparser.ConfigParser()
  conf.read(configfile)
  
  now = startString=datetime.datetime.now().isoformat()

  if os.path.exists(conf['main']['datefile']):
    startString=readdatefile(conf['main']['datefile'])
  else:
    print "Warning: no datefile.  Using now."
    startString=now
# datetime.datetime.strptime("2007-03-04T21:08:12Z", "%Y-%m-%dT%H:%M:%SZ")
  start = datetime.datetime.strptime(startString,"%Y-%m-%dT%H:%M:%S.%f")
  readlog(conf['main']['logfile'],done)
  readlog(conf['main']['retryfile'],retry)

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
    
  writedatefile(conf['main']['datefile'],now)
