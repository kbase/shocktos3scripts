#!/usr/bin/env python3

'''
This script converts MongoDB records for the Workspace Shock backend into records for the
workspace S3 backend.
The script does not alter the Shock backend records and may be re-run multiple times without issue.

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

CONFIG_WS_OBJECTID = conf['workspace']['ws_objectid'] or '000000000000000000000000'

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

    db = client[CONFIG_MONGO_DATABASE]
    query = {'_id': {'$gt': bson.ObjectId(CONFIG_WS_OBJECTID)}}
    print(query)
    ttl = db[COLLECTION_SHOCK].count_documents(query)
    count = 0
    lastPrint = 'Processed {}/{} records'.format(count, ttl)
    print(lastPrint)
    doc_update_list = []

    for node in db[COLLECTION_SHOCK].find(query, batch_size=10000, no_cursor_timeout=True):

#	doc[KEY_S3_CHKSUM] = node[KEY_SHOCK_CHKSUM]
#	doc[KEY_S3_KEY] = toS3Key(node[KEY_SHOCK_NODE])
#	doc[KEY_S3_SORTED] = True if node.get(KEY_SHOCK_SORTED) else False

        doc_update_list.append(UpdateOne(
	    { KEY_S3_CHKSUM: node[KEY_SHOCK_CHKSUM] },
	    {'$set': {
                KEY_S3_KEY: toS3Key(node[KEY_SHOCK_NODE]),
                KEY_S3_SORTED: True if node.get(KEY_SHOCK_SORTED) else False
	    }},
	    upsert=True
	))

	if len(doc_update_list) % 5000 == 0:
            try:
                update_result = db[COLLECTION_S3].bulk_write(doc_update_list,ordered=False)
            except BulkWriteError as bwe:
                print(bwe.details)
	    pprint(update_result.bulk_api_result)
#            print ('inserted {} records'.format(len(insert_result.inserted_ids)))
            doc_update_list = []

#        db[COLLECTION_S3].update_one(
#            {KEY_S3_CHKSUM: node[KEY_SHOCK_CHKSUM]},
#            {'$set': {
#                KEY_S3_KEY: toS3Key(node[KEY_SHOCK_NODE]),
#                KEY_S3_SORTED: True if node.get(KEY_SHOCK_SORTED) else False}},
#            upsert=True)
#        print(KEY_S3_CHKSUM + ' ' + node[KEY_SHOCK_CHKSUM] + ' ' + KEY_S3_KEY + ' ' + toS3Key(node[KEY_SHOCK_NODE]) + ' ' + KEY_S3_SORTED + ' ' + KEY_SHOCK_SORTED)
        count += 1
        if count % 5000 == 0:
            backspace = '\b' * len(lastPrint)
            lastPrint = 'Processed {}/{} records'.format(count, ttl)
            print(lastPrint)

### to do: do final upsert

    backspace = '\b' * len(lastPrint)
    lastPrint = 'Processed {}/{} records'.format(count, ttl)
    print(lastPrint)

def toS3Key(node):
    return node[0:2] + '/' + node[2:4] + '/' + node[4:6] + '/' + node

if __name__ == '__main__':
    main()
