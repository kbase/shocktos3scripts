#!/usr/bin/env python3

'''
This script verifies S3 workspace objects against MongoDB.

It verifies that, for each document in the s3_objects collection (or equivalent
S3 backend), that an S3 object exists and the MD5 matches.  This can be used to validate
a backup copy of the primary S3 instance (e.g., at a secondary site, or in a cloud S3 instance).

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
import multiprocessing
import ctypes

parser = argparse.ArgumentParser(description='Validate Workspace Mongo records against an S3 store.')
parser.add_argument('--config-file', dest='configfile', required=True,
		    help='Path to config file (INI format). (required)')
parser.add_argument('--start-date', dest='startdate', type=str,
		    help='Override config file start date')
parser.add_argument('--end-date', dest='enddate', type=str,
		    help='Override config file end date')
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

if args.startdate is not None:
    (CONFIG_START_YEAR,CONFIG_START_MONTH,CONFIG_START_DAY) = args.startdate.split('-')
if args.enddate is not None:
    (CONFIG_END_YEAR,CONFIG_END_MONTH,CONFIG_END_DAY) = args.enddate.split('-')

CONFIG_START_DATE = datetime.datetime(int(CONFIG_START_YEAR),int(CONFIG_START_MONTH),int(CONFIG_START_DAY),0,0,0)
CONFIG_END_DATE = datetime.datetime(int(CONFIG_END_YEAR),int(CONFIG_END_MONTH),int(CONFIG_END_DAY),0,0,0)

CONFIG_WS_OBJECTID_START = bson.ObjectId.from_datetime(CONFIG_START_DATE)
CONFIG_WS_OBJECTID_END = bson.ObjectId.from_datetime(CONFIG_END_DATE)

CONFIG_S3_ENDPOINT = conf['s3']['endpoint']
CONFIG_S3_BUCKET = conf['s3']['workspace_bucket']
CONFIG_S3_ACCESS_KEY = conf['s3']['workspace_access_key']
CONFIG_S3_SECRET_KEY = conf['s3']['workspace_secret_key']
CONFIG_S3_REGION = conf['s3']['region']
CONFIG_S3_VERIFY = True
if ('insecure' in conf['s3'].keys() and int(conf['s3']['insecure']) != 0 ):
    CONFIG_S3_VERIFY = False
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    urllib3.disable_warnings(botocore.vendored.requests.packages.urllib3.exceptions.InsecureRequestWarning)

CONFIG_NTHREADS = 1
if ('nthreads' in conf['main'].keys()):
    CONFIG_NTHREADS = int(conf['main']['nthreads'])
CONFIG_BATCH_SIZE = 10000

#### END CONFIGURATION VARIABLES ####

COLLECTION_SHOCK = 'shock_nodeMap'
COLLECTION_S3 = 's3_objects'

KEY_SHOCK_CHKSUM = 'chksum'
KEY_SHOCK_NODE = 'node'

KEY_S3_CHKSUM = 'chksum'
KEY_S3_KEY = 'key'

COLLECTION_SOURCE=COLLECTION_S3
KEY_SOURCEID = KEY_S3_KEY

s3 = boto3.client(
    's3',
    endpoint_url=CONFIG_S3_ENDPOINT,
    aws_access_key_id=CONFIG_S3_ACCESS_KEY,
    aws_secret_access_key=CONFIG_S3_SECRET_KEY,
    region_name=CONFIG_S3_REGION,
    config=bcfg.Config(s3={'addressing_style': 'path'}),
    verify=CONFIG_S3_VERIFY
)

# create vars shared across processes
count_good_s3 = multiprocessing.Value(ctypes.c_int)
#count_good_s3.value = 0
count_bad_s3 = multiprocessing.Value(ctypes.c_int)
#count_bad_s3.value = 0
count_processed = multiprocessing.Value(ctypes.c_int)
#count_processed.value = 0
count_source = multiprocessing.Value(ctypes.c_int)
#count_source = 0

def verifyObject(s3doc):
#        pprint(node)
#        pprint('examining object ' + node[KEY_SOURCEID] + ' in mongo collection ' + COLLECTION_S3)
#        pprint ('in thread %s' % multiprocessing.current_process(), stream=sys.stderr)
        result = 'unknown'

        try:
            s3stat = s3.head_object(Bucket=CONFIG_S3_BUCKET,Key=s3doc['key'])
# use this instead to simulate a 404
#	    s3stat = s3.head_object(Bucket=CONFIG_S3_BUCKET,Key=s3doc['chksum'])
#	    pprint (s3stat)
        except botocore.exceptions.ClientError as e:
# if 404 not found, just note the missing object and continue
            if '404' in str(e):
                with count_bad_s3.get_lock():
                    count_bad_s3.value += 1
#                    count['bad_s3'] += 1
                result = 'bad_s3'
                pprint(COLLECTION_SOURCE + ' node/key ' + s3doc[KEY_SOURCEID] + ' is missing matching object in S3 ' + CONFIG_S3_ENDPOINT)
            else:
# otherwise, something bad happened, raise a real exception
                raise(e)
        else:
#                count['good_s3'] += 1
            with count_good_s3.get_lock():
                count_good_s3.value += 1
            result = 'good_s3'
#        count['processed'] += 1
        with count_processed.get_lock():
            count_processed.value += 1
        if count_processed.value % 1000 == 0:
            lastPrint = 'Processed {}/{} records in thread {}'.format(count_processed.value, count_source.value, multiprocessing.current_process() )
            print(lastPrint)
            pprint('good_s3: {} ; bad_s3: {} ; processed: {} ; {}: {}'.format(count_good_s3.value,count_bad_s3.value,count_processed.value,COLLECTION_SOURCE,count_source.value))
        return result

def main():

    count = dict()
    count['processed'] = 0
    count['good_s3'] = 0
    count['bad_s3'] = 0

    pprint ("verifying workspace S3 against mongo source " + COLLECTION_SOURCE + " for dates " + str(CONFIG_START_DATE) + " to " + str(CONFIG_END_DATE) + ' with ' + str(CONFIG_NTHREADS) + ' threads', stream=sys.stderr)

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
    idQuery = {'_id': {'$gt': CONFIG_WS_OBJECTID_START, '$lt': CONFIG_WS_OBJECTID_END }}
#    pprint(idQuery)
    count_source.value = db[COLLECTION_SOURCE].count_documents(idQuery)
#    count[COLLECTION_SOURCE] = db[COLLECTION_SOURCE].count_documents(idQuery)
    lastPrint = 'Processed {}/{} records in main thread'.format(count_processed.value, count_source.value)
    print(lastPrint)

#    for node in db[COLLECTION_SOURCE].find(idQuery, batch_size=CONFIG_BATCH_SIZE, no_cursor_timeout=True):
    pool = multiprocessing.Pool(processes=CONFIG_NTHREADS)
    results=pool.imap_unordered(verifyObject, db[COLLECTION_SOURCE].find(idQuery, batch_size=CONFIG_BATCH_SIZE, no_cursor_timeout=True), 1000)

#    pprint(results)

# apparently need to iterate over the results to get it to do anything
    for result in results:
#        pass
        count['processed'] += 1
        count[result] += 1

    lastPrint = 'Processed {}/{} records in main thread'.format(count_processed.value, count_source.value)
    print(lastPrint)

    pprint('good_s3: {} ; bad_s3: {} ; processed: {} ; {}: {}'.format(count_good_s3.value,count_bad_s3.value,count_processed.value,COLLECTION_SOURCE,count_source.value))
    pprint(count)

if __name__ == '__main__':
    main()
