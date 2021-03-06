#!/usr/bin/python

'''
This is a beta version modifying our existing synctool.py for Shock sync to do
S3 to S3 sync instead.

To do:
  * better and more organized logging (mostly done)
  * add support for start date on command line?
  * make it possible to use same config file for ws/blobstore?
  * replace calling mc binary with making native python calls?
    * currently mc stdout goes to the script stdout, which is a bit messy
  * better documentation of config file (see syncS3ws.ini in this repo, mostly done)
  * support ws/blobstore mode (done)
  * check for target object and skip if exists (done)
    * verify MD5 too?
    * make a cmdline/config option (in case don't want to spend money and read ops on remote S3)
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
  if (debug):
    pprint(idQuery,stream=sys.stderr)
 
  for object in db[conf['main']['mongo_collection']].find(idQuery):
#    pprint(object)
    ids.append(object[conf['main']['mongo_keyfield']])
    ct+=1
  return ids

def syncnode(id):

  if id in done:
    #writelog(conf['logfile'],id)
    if (debug):
      pprint ("%s found in log, skipping" % (id), stream=sys.stderr)
    return 0

  objectPath=id
  if (conf['main']['mode'] == 'blobstore'):
    objectPath="%s/%s/%s/%s"%(id[0:2],id[2:4],id[4:6],id)

  if (debug):
    pprint ("looking for %s at destination %s" % (id,objectPath) , stream=sys.stderr)
  destStat = dict()
  try:
    destStat = destS3.head_object(Bucket=conf['destination']['bucket'],Key=objectPath)
#    if (debug):
#      pprint("deststat is %s" % deststat)
  except botocore.exceptions.ClientError as e:
# if 404 not found, need to put
    if ('404' in e.message):
      if (debug):
        pprint("%s not found at destination %s"%(id, objectPath) , stream=sys.stderr)
    else:
# otherwise, something bad happened, raise a real exception
      raise(e)
  if ('ETag' in destStat):
    if (debug):
      pprint ("%s found at destination %s with ETag %s, skipping" % (id, objectPath, destStat['ETag']), stream=sys.stderr)
    writelog(conf['main']['logfile'],id)
    return 0

  if (debug):
    pprint ("looking for %s at source %s" % (objectPath,conf['source']['url']) , stream=sys.stderr)

  try:
    sourceStat = sourceS3.head_object(
      Bucket=conf['source']['bucket'],
      Key=objectPath
    )
  except botocore.exceptions.ClientError as e:
# if 404 not found, just skip, likely bad Shock node
    if ('404' in e.message):
      pprint("%s not found at source %s, skipping" % (id, conf['source']['url']) , stream=sys.stderr)
      return 0
    else:
# otherwise, something bad happened, raise a real exception
      raise(e)

  if (debug):
    pprint ("copying %s (size %d) to destination %s %s" % (id,sourceStat['ContentLength'],conf['destination']['url'],objectPath) , stream=sys.stderr)
# ~ 20gb
#  if (int(sourceStat['ContentLength']) > 20000000000):
# ~ 5gb
  if (int(sourceStat['ContentLength']) > 5000000000):
    pprint ("object %s is huge, size %d, skipping" % (id,sourceStat['ContentLength']) )
    return 1

  try:
    sourceObject = sourceS3.get_object(
      Bucket=conf['source']['bucket'],
      Key=objectPath
    )
  except botocore.exceptions.ClientError as e:
      raise(e)

# leave early when debugging
#  return 0

  try:
    destResult = destS3.upload_fileobj(
      sourceObject['Body'],
      conf['destination']['bucket'],
      objectPath,
      ExtraArgs={ 'Metadata': sourceObject['Metadata'] }
    )

#   destResult = destS3.put_object(
#     Bucket=conf['destination']['bucket'],
#     Key=objectPath,
#     Body=sourceObject['Body'].read(),
#     Metadata=sourceObject['Metadata']
#   )
    writelog(conf['main']['logfile'],id)
    result = 0
  except botocore.exceptions.ClientError as e:
    # not sure what to do here yet
    pprint(e.message, stream=sys.stderr)
    pprint("id %s failed to copy, writing to retry file"%(id) , stream=sys.stderr)
    writelog(conf['main']['retryfile'],id)
    result = 1
#    raise(e)
   
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
    pprint("Warning: no datefile.  Using now." , stream=sys.stderr)
    startString=now
# datetime.datetime.strptime("2007-03-04T21:08:12Z", "%Y-%m-%dT%H:%M:%SZ")
  start = datetime.datetime.strptime(startString,"%Y-%m-%dT%H:%M:%S.%f")
  end = datetime.datetime.strptime(now,"%Y-%m-%dT%H:%M:%S.%f")
  if (args.startdate):
    start = datetime.datetime.strptime(args.startdate,"%Y-%m-%dT%H:%M:%S.%f")
  if (args.enddate):
    end = datetime.datetime.strptime(args.enddate,"%Y-%m-%dT%H:%M:%S.%f")
  sslVerifySource = True
  sslVerifyDest = True

  if int(conf['source']['insecure']) == 1:
    sslVerifySource = False
    import botocore.vendored.requests.packages.urllib3 as urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
  if int(conf['destination']['insecure']) == 1:
    sslVerifyDest = False
    import botocore.vendored.requests.packages.urllib3 as urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

  readlog(conf['main']['logfile'],done)
  readlog(conf['main']['retryfile'],retry)

  if int(conf['main']['debug'])==1:
    debug=1

  if debug:
    pprint('sslVerifySource = %s , sslVerifyDest = %s' % (str(sslVerifySource),str(sslVerifyDest)), stream=sys.stderr)

  sourceS3 = boto3.client(
        's3',
        endpoint_url=conf['source']['url'],
        aws_access_key_id=conf['source']['accessKey'],
        aws_secret_access_key=conf['source']['secretKey'],
        region_name=conf['source']['region'],
        config=bcfg.Config(s3={'addressing_style': 'path'}),
	verify=sslVerifySource
    )

  destS3 = boto3.client(
        's3',
        endpoint_url=conf['destination']['url'],
        aws_access_key_id=conf['destination']['accessKey'],
        aws_secret_access_key=conf['destination']['secretKey'],
        region_name=conf['destination']['region'],
        config=bcfg.Config(s3={'addressing_style': 'path'}),
	verify=sslVerifyDest
    )

  pprint ('start = %s'%(start) , stream=sys.stderr)
  pprint ('end = %s'%(end) , stream=sys.stderr)

  if debug:
    pprint("querying mongo", stream=sys.stderr)
  objectList=getObjects(start, end)
  # TODO append retry to objectList
  for item in retry:
    objectList.append(item)

  pprint ('objectcount = %d'%(len(objectList)) , stream=sys.stderr)
  if int(conf['main']['resetlog'])==1:
    initlog(conf['main']['logfile'])  

  initlog(conf['main']['retryfile'])

  pool = Pool(processes=int(conf['main']['nthreads']))
  results=pool.map(syncnode, objectList)
  failed=0
  for result in results:
    if result:
      failed+=1
  pprint("failed = %d"%(failed) , stream=sys.stderr)
  pprint("==========" , stream=sys.stderr)
    
  writedatefile(conf['main']['datefile'],now)
