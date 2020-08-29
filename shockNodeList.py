#!/usr/bin/env python3

import datetime

###### CONFIGURATION VARIABLES ######

CONFIG_MONGO_HOST_SRC = 'next-mongo'
CONFIG_MONGO_DATABASE_SRC = 'shock'
CONFIG_MONGO_USER_SRC = 'shock'
CONFIG_MONGO_PWD_SRC = 'shock4me'

CONFIG_START_DATE = datetime.datetime(2020,8,1,0,0,0)
CONFIG_END_DATE = datetime.datetime(2020,10,1,0,0,0)

#### END CONFIGURATION VARIABLES ####

from pymongo.mongo_client import MongoClient

COLLECTION_SHOCK = 'Nodes'

def main():
    if CONFIG_MONGO_USER_SRC:
        client_src = MongoClient(CONFIG_MONGO_HOST_SRC, authSource=CONFIG_MONGO_DATABASE_SRC, username=CONFIG_MONGO_USER_SRC, password=CONFIG_MONGO_PWD_SRC)
    else:
        client_src = MongoClient(CONFIG_MONGO_HOST_SRC)

    db_src = client_src[CONFIG_MONGO_DATABASE_SRC]

    for node in db_src[COLLECTION_SHOCK].find({'created_on': {'$gt': CONFIG_START_DATE, '$lt': CONFIG_END_DATE}},batch_size=10000,no_cursor_timeout=True):
        #print(node['id'])
        print (node['id'][0:2] + '/' + node['id'][2:4] + '/' + node['id'][4:6] + '/' + node['id'] + '/' + node['id'] + '.data')


if __name__ == '__main__':
    main()
