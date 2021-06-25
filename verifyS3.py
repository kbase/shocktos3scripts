#!/usr/bin/env python3

'''
This script verifies S3 workspace or blobstore objects against MongoDB.

It verifies that, for each document in the source collection (workspace s3_objects
or blobstore nodes), that the object exists in S3 and the MD5 matches.  This can be
used to validate any copy of the primary S3 instance (e.g., at a secondary site,
or in a cloud S3 instance, or even the primary S3 instance).

NOTE 1: before February 2020, some blobstore nodes may be missing in the target S3
due to bugs in Shock.  These can generally be ignored.  

NOTE 2: before February 2020, copying data from Shock to Minio resulted in some
chunked uploads (for a subset of objects > 128MB), which makes that object's ETag
different from the MD5 recorded in MongoDB. (The ETag is calculated by checksumming
the checksums of the individual chunks, then a `-NN` label is added to the end to
denote how many chunks there were.)  There may not be an easy way to have Minio
recalculate the MD5 on the full file, so those ETags will cause an MD5 mismatch too.
(New objects created by workspace, blobstore, or the current sync scripts in this
repo should not have this issue.)

NOTE 3: before June 2018, a small percentage of objects were not saved properly, causing
an MD5 mismatch between MongoDB and the object store (see JIRA issue SCT-636).  Unless
a user complains, these errors can be ignored.

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

parser = argparse.ArgumentParser(description='Validate workspace or blobstore mongo records against an S3 store.')
parser.add_argument('--config-file', dest='configfile', required=True,
		    help='Path to config file (INI format). (required)')
parser.add_argument('--start-date', dest='startdate', type=str,
		    help='Override config file start date')
parser.add_argument('--end-date', dest='enddate', type=str,
		    help='Override config file end date')
parser.add_argument('--source-mode', dest='sourcemode', required=True,
		    help='Verify either workspace or blobstore as source mongo. (required)')
args = parser.parse_args()

configfile=args.configfile
conf=configparser.ConfigParser()
conf.read(configfile)

###### CONFIGURATION VARIABLES ######

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

CONFIG_S3_ENDPOINT = conf['s3']['endpoint']
CONFIG_S3_REGION = conf['s3']['region']
CONFIG_S3_VERIFYCERT = True
if ('insecure' in conf['s3'].keys() and int(conf['s3']['insecure']) != 0 ):
    CONFIG_S3_VERIFYCERT = False
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    urllib3.disable_warnings(botocore.vendored.requests.packages.urllib3.exceptions.InsecureRequestWarning)

CONFIG_NTHREADS = 1
if ('nthreads' in conf['main'].keys()):
    CONFIG_NTHREADS = int(conf['main']['nthreads'])
CONFIG_BATCH_SIZE = 10000

if (args.sourcemode == 'workspace'):
    CONFIG_S3_BUCKET = conf['s3']['workspace_bucket']
    CONFIG_S3_ACCESS_KEY = conf['s3']['workspace_access_key']
    CONFIG_S3_SECRET_KEY = conf['s3']['workspace_secret_key']
    s3 = boto3.client(
        's3',
        endpoint_url=CONFIG_S3_ENDPOINT,
        aws_access_key_id=CONFIG_S3_ACCESS_KEY,
        aws_secret_access_key=CONFIG_S3_SECRET_KEY,
        region_name=CONFIG_S3_REGION,
        config=bcfg.Config(s3={'addressing_style': 'path'}),
        verify=CONFIG_S3_VERIFYCERT
    )
    COLLECTION_SOURCE='s3_objects'
    OBJID_KEY = 'key'
    CHKSUM_KEY = 'chksum'
    CONFIG_MONGO_HOST = conf['workspace']['mongo_host']
    CONFIG_MONGO_DATABASE = conf['workspace']['mongo_database']
    CONFIG_MONGO_USER = conf['workspace']['mongo_user']
    CONFIG_MONGO_PWD = conf['workspace']['mongo_pwd']
elif (args.sourcemode == 'blobstore'):
    CONFIG_S3_BUCKET = conf['s3']['blobstore_bucket']
    CONFIG_S3_ACCESS_KEY = conf['s3']['blobstore_access_key']
    CONFIG_S3_SECRET_KEY = conf['s3']['blobstore_secret_key']
    s3 = boto3.client(
        's3',
        endpoint_url=CONFIG_S3_ENDPOINT,
        aws_access_key_id=CONFIG_S3_ACCESS_KEY,
        aws_secret_access_key=CONFIG_S3_SECRET_KEY,
        region_name=CONFIG_S3_REGION,
        config=bcfg.Config(s3={'addressing_style': 'path'}),
        verify=CONFIG_S3_VERIFYCERT
    )
    COLLECTION_SOURCE='nodes'
    OBJID_KEY = 'id'
    CHKSUM_KEY = 'md5'
    CONFIG_MONGO_HOST = conf['blobstore']['mongo_host']
    CONFIG_MONGO_DATABASE = conf['blobstore']['mongo_database']
    CONFIG_MONGO_USER = conf['blobstore']['mongo_user']
    CONFIG_MONGO_PWD = conf['blobstore']['mongo_pwd']
else:
    sys.exit("Invalid --source-mode specified!")

#### END CONFIGURATION VARIABLES ####

# create vars shared across processes
count_good_s3 = multiprocessing.Value(ctypes.c_int)
count_missing_s3 = multiprocessing.Value(ctypes.c_int)
count_md5_mismatch = multiprocessing.Value(ctypes.c_int)
count_processed = multiprocessing.Value(ctypes.c_int)
count_source = multiprocessing.Value(ctypes.c_int)

def verifyObject(obj):
#        pprint(obj)
#        pprint('examining object ' + obj[KEY_SOURCEID] + ' in mongo collection ' + COLLECTION_S3)
#        pprint ('in thread %s' % multiprocessing.current_process(), stream=sys.stderr)
        result = 'unknown'

        s3path = obj[OBJID_KEY]
        if (args.sourcemode == 'blobstore'):
            id = obj[OBJID_KEY]
            s3path="%s/%s/%s/%s"%(id[0:2],id[2:4],id[4:6],id)

        try:
            s3stat = s3.head_object(Bucket=CONFIG_S3_BUCKET,Key=s3path)
# use this instead to simulate a 404
#	    s3stat = s3.head_object(Bucket=CONFIG_S3_BUCKET,Key=obj[CHKSUM])
        except botocore.exceptions.ClientError as e:
# if 404 not found, just note the missing object and continue
            if '404' in str(e):
                with count_missing_s3.get_lock():
                    count_missing_s3.value += 1
                result = 'missing_s3'
                print('{} object {} is missing matching object in S3 {}'.format(COLLECTION_SOURCE, obj[OBJID_KEY],CONFIG_S3_ENDPOINT))
            else:
# otherwise, something bad happened, raise a real exception
                raise(e)
        else:
# in minio and google cloud, the HTTP response has double quotes around the Etag value, making comparing weird
            if ( s3stat['ETag'] != '"{}"'.format(obj[CHKSUM_KEY]) ):
#                pprint(s3stat)
#                pprint(str(s3stat['ETag']))
#                pprint(obj[CHKSUM_KEY])
                with count_md5_mismatch.get_lock():
                    count_md5_mismatch.value += 1
                result = 'md5_mismatch'
                extra_mesg = '(may be bad upload before SCT-636)'
                if '-' in s3stat['ETag']:
                    extra_mesg = '(may have been chunked)'
                print('{} object {} has existing object in S3 {} but MD5s do not match {}'.format(
		    COLLECTION_SOURCE, obj[OBJID_KEY],CONFIG_S3_ENDPOINT,extra_mesg))
                print('{} ETag: {} ; {} {} key: {} '.format(CONFIG_S3_ENDPOINT, s3stat['ETag'], COLLECTION_SOURCE, CHKSUM_KEY, obj[CHKSUM_KEY]))
            else:
                with count_good_s3.get_lock():
                    count_good_s3.value += 1
                result = 'good_s3'
        with count_processed.get_lock():
            count_processed.value += 1
        if count_processed.value % 1000 == 0:
            lastPrint = 'Processed {}/{} records in thread {}'.format(count_processed.value, count_source.value, multiprocessing.current_process() )
            print(lastPrint)
            pprint('missing_s3: {} ; md5_mismatch: {} ; good_s3: {} ; processed: {} ; {}: {}'.format(
		    count_missing_s3.value,count_md5_mismatch.value,count_good_s3.value,count_processed.value,COLLECTION_SOURCE,count_source.value))
        return result

def main():

    count = dict()
    count['processed'] = 0
    count['good_s3'] = 0
    count['missing_s3'] = 0
    count['md5_mismatch'] = 0

    pprint ("verifying S3 instance " + CONFIG_S3_ENDPOINT + " against " + args.sourcemode + " mongo source collection " + COLLECTION_SOURCE + " for dates " + str(CONFIG_START_DATE) + " to " + str(CONFIG_END_DATE) + ' with ' + str(CONFIG_NTHREADS) + ' threads', stream=sys.stderr)

    if CONFIG_MONGO_USER:
        client = MongoClient(CONFIG_MONGO_HOST, authSource=CONFIG_MONGO_DATABASE,
            username=CONFIG_MONGO_USER, password=CONFIG_MONGO_PWD, retryWrites=False)
    else:
        client = MongoClient(CONFIG_MONGO_HOST)

    db = client[CONFIG_MONGO_DATABASE]

    if (args.sourcemode == 'workspace'):
        idQuery = {'_id': {'$gt': bson.ObjectId.from_datetime(CONFIG_START_DATE) , '$lt': bson.ObjectId.from_datetime(CONFIG_END_DATE)} }
    elif (args.sourcemode == 'blobstore'):
        idQuery = {'time': {'$gt': CONFIG_START_DATE , '$lt': CONFIG_END_DATE} }
#    pprint(idQuery, stream=sys.stderr)
    count_source.value = db[COLLECTION_SOURCE].count_documents(idQuery)
    count[COLLECTION_SOURCE] = db[COLLECTION_SOURCE].count_documents(idQuery)
    lastPrint = 'Processed {}/{} records in main thread'.format(count_processed.value, count_source.value)
    print(lastPrint)

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

    pprint('missing_s3: {} ; md5_mismatch: {} ; good_s3: {} ; processed: {} ; {}: {}'.format(
	    count_missing_s3.value,count_md5_mismatch.value,count_good_s3.value,count_processed.value,COLLECTION_SOURCE,count_source.value))
    pprint(count)

if __name__ == '__main__':
    main()
