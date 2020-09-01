#!/usr/bin/env python3

import datetime
import configparser
import argparse

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

CONFIG_SHOCK_WS_UUID = '000000'

# dumb but lazy
CONFIG_START_YEAR = conf['shock']['start_year'] or 2000
CONFIG_START_MONTH = conf['shock']['start_month'] or 1
CONFIG_START_DAY = conf['shock']['start_day'] or 1
CONFIG_END_YEAR = conf['shock']['end_year'] or 2037
CONFIG_END_MONTH = conf['shock']['end_month'] or 12
CONFIG_END_DAY = conf['shock']['end_day'] or 28

CONFIG_START_DATE = datetime.datetime(int(CONFIG_START_YEAR),int(CONFIG_START_MONTH),int(CONFIG_START_DAY),0,0,0)
CONFIG_END_DATE = datetime.datetime(int(CONFIG_END_YEAR),int(CONFIG_END_MONTH),int(CONFIG_END_DAY),0,0,0)

#### END CONFIGURATION VARIABLES ####

from pymongo.mongo_client import MongoClient

COLLECTION_SHOCK = 'Nodes'

def main():
    if CONFIG_MONGO_SHOCK_USER:
        client_src = MongoClient(CONFIG_MONGO_SHOCK_HOST, authSource=CONFIG_MONGO_SHOCK_DATABASE, username=CONFIG_MONGO_SHOCK_USER, password=CONFIG_MONGO_SHOCK_PWD)
    else:
        client_src = MongoClient(CONFIG_MONGO_SHOCK_HOST)

    db_src = client_src[CONFIG_MONGO_SHOCK_DATABASE]

    for node in db_src[COLLECTION_SHOCK].find({'created_on': {'$gt': CONFIG_START_DATE, '$lt': CONFIG_END_DATE}},batch_size=10000,no_cursor_timeout=True):
        #print(node['id'])
        print (node['id'][0:2] + '/' + node['id'][2:4] + '/' + node['id'][4:6] + '/' + node['id'] + '/' + node['id'] + '.data')


if __name__ == '__main__':
    main()
