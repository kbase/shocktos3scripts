#!/usr/bin/env python3

'''
This script converts a subset of Shock (https://github.com/kbase/Shock) node records to
BlobStore (https://github.com/kbase/blobstore) records in MongoDB only.
The script does not alter the Shock backend records and may be re-run multiple times without issue.
It also does not copy any files to S3.

The script expects key names based on a uuid in the same way Shock directories are
based on the UUID; for example the UUID 06f5d3ec-8ebf-4d32-8c1c-41e27e40b7fd would be

06/f5/d3/06f5d3ec-8ebf-4d32-8c1c-41e27e40b7fd

... as a key in S3 or a directory tree in Shock.

To use:
1) Start the blobstore at least once so that the proper indexes are created in MongoDB.
2) Fill in the configuration variables below and run the script normally.
'''

import configparser
import argparse
import datetime
import uuid
from pprint import pprint
from pymongo.mongo_client import MongoClient
from pymongo import UpdateOne
from pymongo.errors import ServerSelectionTimeoutError
from pymongo.errors import BulkWriteError

parser = argparse.ArgumentParser(description='Import Shock Mongo data to blobstore Mongo.')
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

CONFIG_MONGO_BLOBSTORE_HOST = conf['blobstore']['mongo_host']
CONFIG_MONGO_BLOBSTORE_DATABASE = conf['blobstore']['mongo_database']
CONFIG_MONGO_BLOBSTORE_USER = conf['blobstore']['mongo_user']
CONFIG_MONGO_BLOBSTORE_PWD = conf['blobstore']['mongo_pwd']

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

CONFIG_BATCH_SIZE = 10000

#### END CONFIGURATION VARIABLES ####

BS_COL_NODES = 'nodes'
BS_COL_USERS = 'users'

BS_KEY_USERS_ID = 'id'
BS_KEY_USERS_USER = 'user'

BS_KEY_NODES_ID = 'id'
BS_KEY_NODES_OWNER = 'own'
BS_KEY_NODES_READERS = 'read'
BS_KEY_NODES_FILENAME = 'fname'
BS_KEY_NODES_FORMAT = 'fmt'
BS_KEY_NODES_SIZE = 'size'
BS_KEY_NODES_MD5 = 'md5'
BS_KEY_NODES_STORED = 'time'
BS_KEY_NODES_PUBLIC = 'pub'


SHOCK_COL_NODES = 'Nodes'
SHOCK_COL_USERS = 'Users'

SHOCK_KEY_USERS_ID = 'uuid'
SHOCK_KEY_USERS_USER = 'username'

SHOCK_KEY_NODES_ID = 'id'
SHOCK_KEY_NODES_CREATED = 'created_on'
SHOCK_KEY_NODES_FILE = 'file'
SHOCK_KEY_NODES_FILE_NAME = 'name'
SHOCK_KEY_NODES_FILE_SIZE = 'size'
SHOCK_KEY_NODES_FILE_CHKSUM = 'checksum'
SHOCK_KEY_NODES_FILE_CHKSUM_MD5 = 'md5'
SHOCK_KEY_NODES_FILE_FORMAT = 'format'
SHOCK_KEY_NODES_ACLS = 'acl'
SHOCK_KEY_NODES_ACLS_OWNER = 'owner'
SHOCK_KEY_NODES_ACLS_READERS = 'read'
SHOCK_KEY_NODES_ACLS_PUBLIC = 'public'

def main():
    shockdb = get_mongo_client(CONFIG_MONGO_SHOCK_HOST, CONFIG_MONGO_SHOCK_DATABASE,
        CONFIG_MONGO_SHOCK_USER, CONFIG_MONGO_SHOCK_PWD)[CONFIG_MONGO_SHOCK_DATABASE]
    bsdb = get_mongo_client(CONFIG_MONGO_BLOBSTORE_HOST, CONFIG_MONGO_BLOBSTORE_DATABASE,
        CONFIG_MONGO_BLOBSTORE_USER, CONFIG_MONGO_BLOBSTORE_PWD)[CONFIG_MONGO_BLOBSTORE_DATABASE]

    query = { 'acl.owner': { '$ne': CONFIG_SHOCK_WS_UUID }, 'created_on': { '$gt': CONFIG_START_DATE, '$lt': CONFIG_END_DATE } }
    #    print(query)

    totalNodes = shockdb[SHOCK_COL_NODES].count_documents(query)

    count = 0
    seenusers = {}
    doc_update_list = []
    lastPrint = 'Processed {}/{} records'.format(count,totalNodes)
    print (lastPrint)

    for node in shockdb[SHOCK_COL_NODES].find(query,batch_size=CONFIG_BATCH_SIZE,no_cursor_timeout=True):
#        print(node['id'])
        #print (node['id'][0:2] + '/' + node['id'][2:4] + '/' + node['id'][4:6] + '/' + node['id'] + '/' + node['id'] + '.data')

	bsnode = toBSNode(node, seenusers, shockdb, bsdb)
#	pprint(bsnode)
        doc_update_list.append(UpdateOne(
            { BS_KEY_NODES_ID: node['id'] } ,
	    { '$set': bsnode }   
		 , upsert=True
	))

#        bsdb[BS_COL_NODES].update_one({BS_KEY_NODES_ID: node}, {'$set': bsnode}, upsert=True)
        count += 1

	if len(doc_update_list) % CONFIG_BATCH_SIZE == 0:
	    bulk_update_blobstore_nodes(bsdb, doc_update_list)
            doc_update_list = []

	if count % CONFIG_BATCH_SIZE == 0:
            lastPrint = 'Processed {}/{} records'.format(count,totalNodes)
            print (lastPrint)

# last update
    if len(doc_update_list) > 0:
        bulk_update_blobstore_nodes(bsdb, doc_update_list)
        lastPrint = 'Processed {}/{} records'.format(count,totalNodes)
        print(lastPrint)

def toBSNode(shocknode, seenusers, shockdb, bsdb):
    n = shocknode
#    pprint(n['id'])
    owner = n[SHOCK_KEY_NODES_ACLS][SHOCK_KEY_NODES_ACLS_OWNER]
    try:
        md5 = n[SHOCK_KEY_NODES_FILE][SHOCK_KEY_NODES_FILE_CHKSUM][SHOCK_KEY_NODES_FILE_CHKSUM_MD5]
    except KeyError:
	pprint('node ' + n['id'] + ' has no md5, assigning None')
	md5 = None
    readers = n[SHOCK_KEY_NODES_ACLS][SHOCK_KEY_NODES_ACLS_READERS]
    pub = SHOCK_KEY_NODES_ACLS_PUBLIC in readers
    while SHOCK_KEY_NODES_ACLS_PUBLIC in readers: readers.remove(SHOCK_KEY_NODES_ACLS_PUBLIC)  

    bsnode = {
        BS_KEY_NODES_ID: n[SHOCK_KEY_NODES_ID],
        BS_KEY_NODES_OWNER: get_user(owner, seenusers, shockdb, bsdb),
        BS_KEY_NODES_READERS: [get_user(r, seenusers, shockdb, bsdb) for r in readers],
        BS_KEY_NODES_STORED: n[SHOCK_KEY_NODES_CREATED],
        BS_KEY_NODES_FILENAME: n[SHOCK_KEY_NODES_FILE][SHOCK_KEY_NODES_FILE_NAME],
        BS_KEY_NODES_SIZE: n[SHOCK_KEY_NODES_FILE][SHOCK_KEY_NODES_FILE_SIZE],
        BS_KEY_NODES_FORMAT: n[SHOCK_KEY_NODES_FILE][SHOCK_KEY_NODES_FILE_FORMAT],
        BS_KEY_NODES_MD5: md5,
        BS_KEY_NODES_PUBLIC: pub,
    }
    return bsnode

def get_user(uuid, seenusers, shockdb, bsdb):
    if uuid in seenusers:
        return {BS_KEY_USERS_ID: uuid, BS_KEY_USERS_USER: seenusers[uuid]}
    
    u = shockdb[SHOCK_COL_USERS].find_one({SHOCK_KEY_USERS_ID: uuid})
    if not u:
        raise ValueError('Missing user in Shock ' + uuid)
    bsdb[BS_COL_USERS].update_one(
        {BS_KEY_USERS_ID: uuid},
        {'$set': {BS_KEY_USERS_USER: u[SHOCK_KEY_USERS_USER]}},
        upsert=True)
    seenusers[uuid] = u[SHOCK_KEY_USERS_USER]
    return {BS_KEY_USERS_ID: uuid, BS_KEY_USERS_USER: u[SHOCK_KEY_USERS_USER]}
    
def toUUID(s3key):
    u = s3key.split('/')
    uuidStr = u[3]
    if uuidStr[0:2] != u[0] or uuidStr[2:4] != u[1] or uuidStr[4:6] != u[2]:
        raise ValueError("Illegal S3 key: " + uuidStr)
    try:
        uuid.UUID(hex=uuidStr)
    except TypeError as _:
        raise ValueError("Illegal S3 key: " + uuidStr)
    return uuidStr
    
def get_mongo_client(host, db, user, pwd):
    if user:
        return MongoClient(host, authSource=db, username=user, password=pwd, retryWrites=False)
    else:
        return MongoClient(host, retryWrites=False)


def bulk_update_blobstore_nodes(db, doc_update_list):
    try:
        update_result = db[BS_COL_NODES].bulk_write(doc_update_list,ordered=False)
    except BulkWriteError as bwe:
        print(bwe.details)
    print('nInserted: ' + str(update_result.bulk_api_result['nInserted']) + ' ; nUpserted: ' + str(update_result.bulk_api_result['nUpserted']) + ' ; writeErrors: ' + str(update_result.bulk_api_result['writeErrors']))
#    pprint(update_result.bulk_api_result)

if __name__ == '__main__':
    main()

