#!/usr/bin/env python3

'''
This script converts MongoDB records for the Workspace Shock backend into records for the
workspace S3 backend.
The script does not alter the Shock backend records and may be re-run multiple times without issue.
The script also does not add resources to the S3 backend.

To run:
1) Start the workspace at least once with the S3 backend enabled to create the appropriate
    MongoDB indexes.
    ( or run `db.s3_objects.createIndex({chksum:1},{unique:1})` in the workspace db (untested) )
2) fill in the configuration variables for mongo DB below and run the script normally.
'''

from pymongo.mongo_client import MongoClient
from pymongo.errors import ServerSelectionTimeoutError
from pymongo.errors import BulkWriteError
from pymongo import UpdateOne
from pprint import pprint
import bson
import configparser
import argparse
import datetime

parser = argparse.ArgumentParser(description='Import workspace Mongo data to blobstore Mongo.')
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

# can use a different db for destination
CONFIG_MONGO_S3_HOST = conf['s3']['mongo_host']
CONFIG_MONGO_S3_DATABASE = conf['s3']['mongo_database']
CONFIG_MONGO_S3_USER = conf['s3']['mongo_user']
CONFIG_MONGO_S3_PWD = conf['s3']['mongo_pwd']

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

CONFIG_BATCH_SIZE = 10000

#### END CONFIGURATION VARIABLES ####

COLLECTION_SHOCK = 'shock_nodeMap'
COLLECTION_S3 = 's3_objects'

KEY_SHOCK_CHKSUM = 'chksum'
KEY_SHOCK_NODE = 'node'
KEY_SHOCK_SORTED = 'sorted'

KEY_S3_CHKSUM = 'chksum'
KEY_S3_KEY = 'key'
KEY_S3_SORTED = 'sorted'


'''
Potential improvement: allow filtering by > object id. Then you can run this script while
the workspace is up (since it could take hours), figure out the last object id processed, bring
the ws down, and then run with the object id filter to just process the new records.
'''
def main():
    if CONFIG_MONGO_USER:
        client = MongoClient(CONFIG_MONGO_HOST, authSource=CONFIG_MONGO_DATABASE,
            username=CONFIG_MONGO_USER, password=CONFIG_MONGO_PWD, retryWrites=False)
    else:
        client = MongoClient(CONFIG_MONGO_HOST)
    if CONFIG_MONGO_S3_USER:
        client_s3 = MongoClient(CONFIG_MONGO_S3_HOST, authSource=CONFIG_MONGO_S3_DATABASE,
            username=CONFIG_MONGO_S3_USER, password=CONFIG_MONGO_S3_PWD, retryWrites=False)
    else:
        client_s3 = MongoClient(CONFIG_MONGO_S3_HOST)

    db = client[CONFIG_MONGO_DATABASE]
    db_s3 = client_s3[CONFIG_MONGO_S3_DATABASE]
    query = {'_id': {'$gt': CONFIG_WS_OBJECTID_START, '$lt': CONFIG_WS_OBJECTID_END }}
    print(query)
    ttl = db[COLLECTION_SHOCK].count_documents(query)
    count = 0
    lastPrint = 'Processed {}/{} records'.format(count, ttl)
    print(lastPrint)
    doc_update_list = []

    for node in db[COLLECTION_SHOCK].find(query, batch_size=CONFIG_BATCH_SIZE, no_cursor_timeout=True):

#	doc[KEY_S3_CHKSUM] = node[KEY_SHOCK_CHKSUM]
#	doc[KEY_S3_KEY] = toS3Key(node[KEY_SHOCK_NODE])
#	doc[KEY_S3_SORTED] = True if node.get(KEY_SHOCK_SORTED) else False

        doc_update_list.append(UpdateOne(
	    { KEY_S3_CHKSUM: node[KEY_SHOCK_CHKSUM] },
	    {'$set': {
		'_id': node['_id'],
                KEY_S3_KEY: toS3Key(node[KEY_SHOCK_NODE]),
                KEY_S3_SORTED: True if node.get(KEY_SHOCK_SORTED) else False
	    }},
	    upsert=True
	))

        count += 1

	if len(doc_update_list) % CONFIG_BATCH_SIZE == 0:
	    bulk_update(db_s3, doc_update_list)
            doc_update_list = []

        if count % CONFIG_BATCH_SIZE == 0:
            lastPrint = 'Processed {}/{} records'.format(count, ttl)
            print(lastPrint)

# final docs
    bulk_update(db_s3, doc_update_list)
    lastPrint = 'Processed {}/{} records'.format(count, ttl)
    print(lastPrint)


def toS3Key(node):

# for CI and next ws only, using original bulkS3upload that doesn't munge the object path
#    return node[0:2] + '/' + node[2:4] + '/' + node[4:6] + '/' + node + '/' + node + '.data'
# for future imports, use the path ws and blobstore expect
    return node[0:2] + '/' + node[2:4] + '/' + node[4:6] + '/' + node


def bulk_update(db, doc_update_list):
    try:
        update_result = db[COLLECTION_S3].bulk_write(doc_update_list,ordered=False)
    except BulkWriteError as bwe:
        print(bwe.details)
    print('nInserted: ' + str(update_result.bulk_api_result['nInserted']) + ' ; nUpserted: ' + str(update_result.bulk_api_result['nUpserted']) + ' ; writeErrors: ' + str(update_result.bulk_api_result['writeErrors']))
#    pprint(update_result.bulk_api_result)


if __name__ == '__main__':
    main()
