#!/usr/bin/env python3

'''
This script verifies that old and converted MongoDB records for the blobstore,
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

parser = argparse.ArgumentParser(description='Validate Shock Mongo records against blobstore and an S3 store.')
parser.add_argument('--config-file', dest='configfile', required=True,
		    help='Path to config file (INI format). (required)')
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
CONFIG_S3_ACCESS_KEY = conf['s3']['access_key']
CONFIG_S3_SECRET_KEY = conf['s3']['secret_key']
CONFIG_S3_REGION = conf['s3']['region']

CONFIG_BATCH_SIZE = 10000

#### END CONFIGURATION VARIABLES ####

COLLECTION_SHOCK = 'Nodes'
COLLECTION_BLOBSTORE = 'nodes'

def main():
    s3 = boto3.client(
        's3',
        endpoint_url=CONFIG_S3_ENDPOINT,
        aws_access_key_id=CONFIG_S3_ACCESS_KEY,
        aws_secret_access_key=CONFIG_S3_SECRET_KEY,
        region_name=CONFIG_S3_REGION,
        config=bcfg.Config(s3={'addressing_style': 'path'})
    )
#    pprint(s3.list_buckets())
#    try:
#        pprint(s3.head_object(Bucket=CONFIG_S3_BUCKET,Key='eb/e5/b8/ebe5b84a-47be-4d49-a54b-fd85fdeb1550/ebe5b84a-47be-4d49-a54b-fd85fdeb1550.dat'))
#    except botocore.exceptions.ClientError as e:
#	pprint(e)

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
    count['missing_md5'] = 0
    count['good_s3'] = 0
    count['bad_s3'] = 0
    count['processed'] = 0

    shockDb = shockClient[CONFIG_MONGO_SHOCK_DATABASE]
    blobstoreDb = blobstoreClient[CONFIG_MONGO_BLOBSTORE_DATABASE]

    shockQuery = { 'acl.owner': { '$ne': CONFIG_SHOCK_WS_UUID }, 'created_on': { '$gt': CONFIG_START_DATE, '$lt': CONFIG_END_DATE } }
#    pprint(shockQuery)
    count[COLLECTION_SHOCK] = shockDb[COLLECTION_SHOCK].count_documents(shockQuery)
#    count = 0
    lastPrint = 'Processed {}/{} records'.format(count['processed'], count[COLLECTION_SHOCK])
    print(lastPrint)

    for node in shockDb[COLLECTION_SHOCK].find(shockQuery, batch_size=CONFIG_BATCH_SIZE, no_cursor_timeout=True):
#        pprint('examining node ' + node['id'] + ' in mongo collection ' + COLLECTION_BLOBSTORE)
	blobstoreQuery = {'id': node['id']}
        blobstoreDoc = blobstoreDb[COLLECTION_BLOBSTORE].find_one(blobstoreQuery)
	if (blobstoreDoc == None):
	    pprint(COLLECTION_SHOCK + ' node ' + node['id'] + ' is missing matching entry in ' + COLLECTION_BLOBSTORE)
	    count['bad_mongo'] += 1
	else:
            count['good_mongo'] += 1
	    if ( node['md5'] == None ):
	        pprint(COLLECTION_SHOCK + ' node ' + node['id'] + ' has no MD5, skipping')
                count['missing_md5'] += 1
	        continue
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
	        if '404' in e.message:
	            count['bad_s3'] += 1
	            pprint(COLLECTION_SHOCK + ' node ' + node['id'] + ' is missing matching object in S3 ' + CONFIG_S3_ENDPOINT)
	        else:
# otherwise, something bad happened, raise a real exception
		    raise(e)
	    else:
                count['good_s3'] += 1
        count['processed'] += 1
	if count['processed'] % 1000 == 0:
	    lastPrint = 'Processed {}/{} records'.format(count['processed'], count[COLLECTION_SHOCK])
	    print(lastPrint)
            pprint(count)

    lastPrint = 'Processed {}/{} records'.format(count['processed'], count[COLLECTION_SHOCK])
    print(lastPrint)

    pprint(count)


if __name__ == '__main__':
    main()
