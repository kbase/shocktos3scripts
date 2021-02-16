#!/usr/bin/python

'''
This is a beta version modifying our existing synctool.py for Shock sync to do
S3 to S3 sync instead.

To do:
  * better documentation of config file
  * make it possible to use same config file for ws/blobstore?
  * support ws/blobstore mode (done)
  * check for target object and skip if exists (done)
    * verify MD5 too?
    * make a cmdline/config option?
  * add support for end date (done)
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
import boto3
import botocore
import botocore.config as bcfg

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

def getObjects(start, end):
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

  idQuery = {'_id': {'$gt': bson.ObjectId.from_datetime(start) , '$lt': bson.ObjectId.from_datetime(end)} }
  if (conf['main']['mode'] == 'blobstore'):
    idQuery = {'time': {'$gt': start , '$lt': end} }
#  idQuery = {'_id': {'$gt': bson.ObjectId.from_datetime(start) } }
  pprint(idQuery)
 
  for object in db[conf['main']['mongo_collection']].find(idQuery):
#    pprint(object)
    ids.append(object[conf['main']['mongo_keyfield']])
    ct+=1
  return ids

def syncnode(id):

  if id in done:
    #writelog(conf['logfile'],id)
    if (debug):
      pprint ("%s found in log, skipping" % (id))
    return 0

  spath="%s/%s/%s"%(conf['source']['endpoint'],conf['source']['bucket'],id)
  dpath="%s/%s/%s"%(conf['destination']['endpoint'],conf['destination']['bucket'],id)
  s3dpath=id
  if (conf['main']['mode'] == 'blobstore'):
    spath="%s/%s/%s/%s/%s/%s"%(conf['source']['endpoint'],conf['source']['bucket'],id[0:2],id[2:4],id[4:6],id)
    dpath="%s/%s/%s/%s/%s/%s"%(conf['destination']['endpoint'],conf['source']['bucket'],id[0:2],id[2:4],id[4:6],id)
    s3dpath="%s/%s/%s/%s"%(id[0:2],id[2:4],id[4:6],id)

  if (debug):
    print "looking for %s at destination %s" % (id,s3dpath)
  deststat = dict()
  try:
    deststat = targetS3.head_object(Bucket=conf['destination']['bucket'],Key=s3dpath)
#    pprint("deststat is %s" % deststat)
  except botocore.exceptions.ClientError as e:
# if 404 not found, need to put
    if '404' in e.message:
      pprint("%s not found at destination %s" % (id, s3dpath))
    else:
# otherwise, something bad happened, raise a real exception
      raise(e)
  if ('ETag' in deststat and debug):
    pprint ("%s found at destination %s with ETag %s, skipping" % (id, s3dpath, deststat['ETag']))
    writelog(conf['main']['logfile'],id)
    return 0

  if (debug):
    print "copying %s to destination " % (id)
  # example from vadmin1:
  # assumes `minio` and `prod-ws01` are defined endpoints in ~/.mc/config.json
  # /opt/mc/mc cp minio/prod-ws/00/00/00/000000e7-0d44-494b-bd17-638f2a904329 prod-ws01.gcp/prod-ws01/00/00/00/000000e7-0d44-494b-bd17-638f2a904329
  comm=(conf['main']['mcpath'],'--quiet','cp',spath,dpath)
# use echo for testing
#  comm=('echo',conf['main']['mcpath'],'--quiet','cp',spath,dpath)
  if (conf['main'].getboolean('insecureminio') == True):
      comm=(conf['main']['mcpath'],'--quiet','--insecure','cp',spath,dpath)
      # use echo for testing
#      comm=('echo',conf['main']['mcpath'],'--quiet','--insecure','cp',spath,dpath)
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
		    help='End date for query in ISO8601 format (optional, default now)')
  parser.add_argument('--start-date', dest='startdate',
		    help='Start date for query in ISO8601 format (optional, default now)')
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
  end = datetime.datetime.strptime(now,"%Y-%m-%dT%H:%M:%S.%f")
  if (args.startdate):
    start = datetime.datetime.strptime(args.startdate,"%Y-%m-%dT%H:%M:%S.%f")
  if (args.enddate):
    end = datetime.datetime.strptime(args.enddate,"%Y-%m-%dT%H:%M:%S.%f")
  readlog(conf['main']['logfile'],done)
  readlog(conf['main']['retryfile'],retry)

  if int(conf['main']['debug'])==1:
    debug=1

  targetS3 = boto3.client(
        's3',
        endpoint_url=conf['destination']['url'],
        aws_access_key_id=conf['destination']['accessKey'],
        aws_secret_access_key=conf['destination']['secretKey'],
        region_name=conf['destination']['region'],
        config=bcfg.Config(s3={'addressing_style': 'path'})
    )

  print >> sys.stderr, 'start=%s'%(start)
  print >> sys.stderr, 'end=%s'%(end)

  if debug:
    print "querying mongo"
  objectList=getObjects(start, end)
  # TODO append retry to objectList
  for item in retry:
    objectList.append(item)

  print >> sys.stderr, 'ct=%d'%(len(objectList))
  if int(conf['main']['resetlog'])==1:
    initlog(conf['main']['logfile'])  

  initlog(conf['main']['retryfile'])

  pool = Pool(processes=int(conf['main']['nthreads']))
  results=pool.map(syncnode, objectList)
  failed=0
  for result in results:
    if result:
      failed+=1
  print "failed: %d"%(failed)
    
  writedatefile(conf['main']['datefile'],now)
