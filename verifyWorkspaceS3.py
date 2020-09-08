#!/usr/bin/env python3

'''
This script verifies that old and converted MongoDB records for the Workspace Shock backend,
and S3 backend resources, all match.  For each record in the shock_nodeMap (or equivalent
Shock backend) collection, it checks for a matching record in the s3_objects (or equivalent
S3 backend) collection, and checks that the Minio resource exists and that it's MD5 matches
the chksum.

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

parser = argparse.ArgumentParser(description='Validate Workspace Mongo records against an S3 store.')
parser.add_argument('--config-file', dest='configfile', required=True,
		    help='Path to config file (INI format). (required)')
args = parser.parse_args()

configfile=args.configfile
conf=configparser.ConfigParser()
conf.read(configfile)

###### CONFIGURATION VARIABLES ######

CONFIG_MONGO_HOST = conf['workspace']['mongo_host']
CONFIG_MONGO_DATABASE = conf['workspace']['mongo_database']
#CONFIG_MONGO_DATABASE = 'workspace_conv_test'
#CONFIG_MONGO_DATABASE = 'workspace_conv_test_many_recs'
CONFIG_MONGO_USER = conf['workspace']['mongo_user']
CONFIG_MONGO_PWD = conf['workspace']['mongo_pwd']

# dumb but lazy
CONFIG_START_YEAR = conf['workspace']['start_year'] or 2000
CONFIG_START_MONTH = conf['workspace']['start_month'] or 1
CONFIG_START_DAY = conf['workspace']['start_day'] or 1
CONFIG_END_YEAR = conf['workspace']['end_year'] or 2037
CONFIG_END_MONTH = conf['workspace']['end_month'] or 12
CONFIG_END_DAY = conf['workspace']['end_day'] or 28

CONFIG_START_DATE = datetime.datetime(int(CONFIG_START_YEAR),int(CONFIG_START_MONTH),int(CONFIG_START_DAY),0,0,0)
CONFIG_END_DATE = datetime.datetime(int(CONFIG_END_YEAR),int(CONFIG_END_MONTH),int(CONFIG_END_DAY),0,0,0)

CONFIG_WS_OBJECTID_START = bson.ObjectId.from_datetime(CONFIG_START_DATE)
CONFIG_WS_OBJECTID_END = bson.ObjectId.from_datetime(CONFIG_END_DATE)

CONFIG_S3_ENDPOINT = conf['s3']['endpoint']
CONFIG_S3_BUCKET = conf['s3']['workspace_bucket']
CONFIG_S3_ACCESS_KEY = conf['s3']['access_key']
CONFIG_S3_SECRET_KEY = conf['s3']['secret_key']
CONFIG_S3_REGION = conf['s3']['region']

CONFIG_BATCH_SIZE = 10000

#### END CONFIGURATION VARIABLES ####

COLLECTION_SHOCK = 'shock_nodeMap'
COLLECTION_S3 = 's3_objects'

KEY_SHOCK_CHKSUM = 'chksum'
KEY_SHOCK_NODE = 'node'

KEY_S3_CHKSUM = 'chksum'
KEY_S3_KEY = 'key'

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

    if CONFIG_MONGO_USER:
        client = MongoClient(CONFIG_MONGO_HOST, authSource=CONFIG_MONGO_DATABASE,
            username=CONFIG_MONGO_USER, password=CONFIG_MONGO_PWD, retryWrites=False)
    else:
        client = MongoClient(CONFIG_MONGO_HOST)

    db = client[CONFIG_MONGO_DATABASE]
    shockQuery = {'_id': {'$gt': CONFIG_WS_OBJECTID_START, '$lt': CONFIG_WS_OBJECTID_END }}
#    pprint(shockQuery)
#    ttl = db[COLLECTION_SHOCK].count_documents(shockQuery)
#    count = 0
#    lastPrint = 'Processed {}/{} records'.format(count, ttl)
#    print(lastPrint)

    count = dict()
    count['good_mongo'] = 0
    count['bad_mongo'] = 0
    count['good_s3'] = 0
    count['bad_s3'] = 0

    for node in db[COLLECTION_SHOCK].find(shockQuery, batch_size=CONFIG_BATCH_SIZE, no_cursor_timeout=True):
        s3Query = {'chksum': node['chksum']}
        s3doc = db[COLLECTION_S3].find_one(s3Query)
	if (s3doc == None):
	    pprint(COLLECTION_SHOCK + ' node ' + node['node'] + ' is missing matching chksum in ' + COLLECTION_S3)
	    count['bad_mongo'] += 1
	else:
            count['good_mongo'] += 1
#	pprint(s3doc)
        try:
	    s3stat = s3.head_object(Bucket=CONFIG_S3_BUCKET,Key=s3doc['key'])
#	    pprint (s3stat)
	except botocore.exceptions.ClientError as e:
# if 404 not found, just note the missing object and continue
	    if '404' in e.message:
	        count['bad_s3'] += 1
	        pprint(COLLECTION_SHOCK + ' node ' + node['node'] + ' is missing matching object in S3 ' + CONFIG_S3_ENDPOINT)
	    else:
# otherwise, something bad happened, raise a real exception
		raise(e)
	else:
            count['good_s3'] += 1

    pprint(count)


if __name__ == '__main__':
    main()
