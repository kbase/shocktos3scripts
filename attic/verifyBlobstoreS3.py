#!/usr/bin/env python3

'''
This script verifies non-workspace (blobstore) objects against MongoDB.  It has two modes,
shock and s3.

In s3 mode, it verifies that, for each document in the blobstore.nodes collection, that an
S3 object exists and the MD5 matches.  This can be used to validate a backup copy of the
primary S3 instance (e.g., at a secondary site, or in a cloud S3 instance).

In shock mode, it verifies that old and converted MongoDB records for the blobstore,
and S3 backend resources, all match.  For each record in the ShockDB.Nodes (or equivalent)
collection, it checks for a matching record in the blobstore.nodes (or equivalent
S3 backend) collection, and checks that the Minio resource exists and that the MD5 records
in Minio and MongoDB all match.

'''

from pymongo.mongo_client import MongoClient
from pprint import pprint
import boto3
import botocore
import botocore.config as bcfg
import bson
import configparser
import argparse
import datetime
import sys

parser = argparse.ArgumentParser(description='Validate Shock Mongo records against blobstore and an S3 store.')
parser.add_argument('--config-file', dest='configfile', required=True,
		    help='Path to config file (INI format). (required)')
parser.add_argument('--mongo-source', dest='mongosource', required=True,
		    help='Which mongo source collection to use as master, shock or s3 . (required)')
parser.add_argument('--start-date', dest='startdate', type=str,
		    help='Override config file start date')
parser.add_argument('--end-date', dest='enddate', type=str,
		    help='Override config file end date')
args = parser.parse_args()

configfile=args.configfile
conf=configparser.ConfigParser()
conf.read(configfile)

###### CONFIGURATION VARIABLES ######

CONFIG_MONGO_SHOCK_HOST = conf['shock']['mongo_host']
CONFIG_MONGO_SHOCK_DATABASE = conf['shock']['mongo_database']
CONFIG_MONGO_SHOCK_USER = conf['shock']['mongo_user']
CONFIG_MONGO_SHOCK_PWD = conf['shock']['mongo_pwd']

CONFIG_SHOCK_WS_UUID = conf['shock']['ws_uuid']

# dumb but lazy
CONFIG_START_YEAR = conf['shock']['start_year'] or 2000
CONFIG_START_MONTH = conf['shock']['start_month'] or 1
CONFIG_START_DAY = conf['shock']['start_day'] or 1
CONFIG_END_YEAR = conf['shock']['end_year'] or 2037
CONFIG_END_MONTH = conf['shock']['end_month'] or 12
CONFIG_END_DAY = conf['shock']['end_day'] or 28

if args.startdate is not None:
    (CONFIG_START_YEAR,CONFIG_START_MONTH,CONFIG_START_DAY) = args.startdate.split('-')
if args.enddate is not None:
    (CONFIG_END_YEAR,CONFIG_END_MONTH,CONFIG_END_DAY) = args.enddate.split('-')

CONFIG_START_DATE = datetime.datetime(int(CONFIG_START_YEAR),int(CONFIG_START_MONTH),int(CONFIG_START_DAY),0,0,0)
CONFIG_END_DATE = datetime.datetime(int(CONFIG_END_YEAR),int(CONFIG_END_MONTH),int(CONFIG_END_DAY),0,0,0)

#CONFIG_WS_OBJECTID_START = bson.ObjectId.from_datetime(CONFIG_START_DATE)
#CONFIG_WS_OBJECTID_END = bson.ObjectId.from_datetime(CONFIG_END_DATE)

CONFIG_MONGO_BLOBSTORE_HOST = conf['blobstore']['mongo_host']
CONFIG_MONGO_BLOBSTORE_DATABASE = conf['blobstore']['mongo_database']
CONFIG_MONGO_BLOBSTORE_USER = conf['blobstore']['mongo_user']
CONFIG_MONGO_BLOBSTORE_PWD = conf['blobstore']['mongo_pwd']

CONFIG_S3_ENDPOINT = conf['s3']['endpoint']
CONFIG_S3_BUCKET = conf['s3']['blobstore_bucket']
CONFIG_S3_ACCESS_KEY = conf['s3']['blobstore_access_key']
CONFIG_S3_SECRET_KEY = conf['s3']['blobstore_secret_key']
CONFIG_S3_REGION = conf['s3']['region']

CONFIG_S3_VERIFYCERT = True
if int(conf['s3']['insecurecert']) == 1:
    CONFIG_S3_VERIFYCERT = False
    import botocore.vendored.requests.packages.urllib3 as urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

CONFIG_BATCH_SIZE = 10000

#### END CONFIGURATION VARIABLES ####

COLLECTION_SHOCK = 'Nodes'
COLLECTION_BLOBSTORE = 'nodes'

if (args.mongosource == 'shock'):
    COLLECTION_SOURCE=COLLECTION_SHOCK
elif (args.mongosource == 's3'):
    COLLECTION_SOURCE=COLLECTION_BLOBSTORE
else:
    raise("invalid mongosource specified! use shock or s3")

def main():

    pprint ("verifying blobstore S3 against mongo source " + args.mongosource + " for dates " + str(CONFIG_START_DATE) + " to " + str(CONFIG_END_DATE), stream=sys.stderr)

    s3 = boto3.client(
        's3',
        endpoint_url=CONFIG_S3_ENDPOINT,
        aws_access_key_id=CONFIG_S3_ACCESS_KEY,
        aws_secret_access_key=CONFIG_S3_SECRET_KEY,
        region_name=CONFIG_S3_REGION,
	verify=CONFIG_S3_VERIFYCERT,
        config=bcfg.Config(s3={'addressing_style': 'path'})
    )

    if CONFIG_MONGO_SHOCK_USER:
        shockClient = MongoClient(CONFIG_MONGO_SHOCK_HOST, authSource=CONFIG_MONGO_SHOCK_DATABASE,
            username=CONFIG_MONGO_SHOCK_USER, password=CONFIG_MONGO_SHOCK_PWD, retryWrites=False)
    else:
        shockClient = MongoClient(CONFIG_MONGO_SHOCK_HOST)
    if CONFIG_MONGO_BLOBSTORE_USER:
        blobstoreClient = MongoClient(CONFIG_MONGO_BLOBSTORE_HOST, authSource=CONFIG_MONGO_BLOBSTORE_DATABASE,
            username=CONFIG_MONGO_BLOBSTORE_USER, password=CONFIG_MONGO_BLOBSTORE_PWD, retryWrites=False)
    else:
        blobstoreClient = MongoClient(CONFIG_MONGO_BLOBSTORE_HOST)

    count = dict()
    count['good_mongo'] = 0
    count['bad_mongo'] = 0
# bad_mongo for s3 checks is irrelevant
    if (args.mongosource == 's3'):
        count['bad_mongo'] = None
    count['missing_md5'] = 0
    count['good_s3'] = 0
    count['bad_s3'] = 0
    count['processed'] = 0

    shockDb = shockClient[CONFIG_MONGO_SHOCK_DATABASE]
    blobstoreDb = blobstoreClient[CONFIG_MONGO_BLOBSTORE_DATABASE]

    shockQuery = { 'acl.owner': { '$ne': CONFIG_SHOCK_WS_UUID }, 'created_on': { '$gt': CONFIG_START_DATE, '$lt': CONFIG_END_DATE } }
#    pprint(shockQuery)
    blobstoreQuery = { 'time': { '$gt': CONFIG_START_DATE, '$lt': CONFIG_END_DATE } }
    if (args.mongosource == 'shock'):
        sourceQuery=shockQuery
        sourceDb=shockDb
    elif (args.mongosource == 's3'):
        sourceQuery=blobstoreQuery
        sourceDb=blobstoreDb

    count[COLLECTION_SOURCE] = sourceDb[COLLECTION_SOURCE].count_documents(sourceQuery)
#    count = 0
    lastPrint = 'Processed {}/{} records'.format(count['processed'], count[COLLECTION_SOURCE])
    print(lastPrint)

    for node in sourceDb[COLLECTION_SOURCE].find(sourceQuery, batch_size=CONFIG_BATCH_SIZE, no_cursor_timeout=True):
#        pprint('examining node ' + node['id'] + ' in mongo collection ' + COLLECTION_BLOBSTORE)
        if (args.mongosource =='shock'):
            blobstoreSingleQuery = {'id': node['id']}
            blobstoreDoc = blobstoreDb[COLLECTION_BLOBSTORE].find_one(blobstoreSingleQuery)
        else:
            blobstoreDoc = node

        if (blobstoreDoc == None):
            print(COLLECTION_SOURCE + ' Shock node ' + node['id'] + ' is missing matching entry in blobstore ' + COLLECTION_BLOBSTORE)
            count['bad_mongo'] += 1
        elif ( blobstoreDoc['md5'] == None ):
            count['good_mongo'] += 1
            print(COLLECTION_SOURCE + ' Shock node ' + node['id'] + ' found in blobstore but has no MD5, skipping S3 verify')
            count['missing_md5'] += 1
        else:
            count['good_mongo'] += 1
#	pprint(s3doc)
#        pprint('examining key ' + s3doc['key'] + ' in S3 endpoint ' + CONFIG_S3_ENDPOINT)
            s3path = ( node['id'][0:2] + '/' + node['id'][2:4] + '/' + node['id'][4:6] + '/' + node['id'] )
            try:
                s3stat = s3.head_object(Bucket=CONFIG_S3_BUCKET,Key=s3path)
# use this instead to simulate a 404
#	    s3stat = s3.head_object(Bucket=CONFIG_S3_BUCKET,Key=s3doc['chksum'])
#	    pprint (s3stat)
            except botocore.exceptions.ClientError as e:
# if 404 not found, just note the missing object and continue
# need to verify fix for python3
                if '404' in str(e):
                    count['bad_s3'] += 1
                    print(COLLECTION_SOURCE + ' node ' + node['id'] + ' is missing matching object in S3 ' + CONFIG_S3_ENDPOINT)
                else:
# otherwise, something bad happened, raise a real exception
                    raise(e)
            else:
                count['good_s3'] += 1
        count['processed'] += 1
        if count['processed'] % 1000 == 0:
            lastPrint = 'Processed {}/{} records'.format(count['processed'], count[COLLECTION_SOURCE])
            print(lastPrint)
            pprint(count)

    lastPrint = 'Processed {}/{} records'.format(count['processed'], count[COLLECTION_SOURCE])
    print(lastPrint)

    pprint(count)


if __name__ == '__main__':
    main()
