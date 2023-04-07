#!/usr/bin/env python3

import datetime
import configparser
import argparse
import sys
from pymongo.mongo_client import MongoClient

parser = argparse.ArgumentParser(description='Import Shock Mongo data to blobstore Mongo.')
parser.add_argument('--config-file', dest='configfile', required=True,
		    help='Path to config file (INI format). (required)')
parser.add_argument('--node-mode', dest='nodemode', required=True,
		    help='Use workspace or shock mode to produce node list. (required)')
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

#### END CONFIGURATION VARIABLES ####

COLLECTION_SHOCK = 'Nodes'

def main():

    print >> sys.stderr, "generating node list for " + args.nodemode + " for dates " + str(CONFIG_START_DATE) + " to " + str(CONFIG_END_DATE)

    if CONFIG_MONGO_SHOCK_USER:
        client_src = MongoClient(CONFIG_MONGO_SHOCK_HOST, authSource=CONFIG_MONGO_SHOCK_DATABASE, username=CONFIG_MONGO_SHOCK_USER, password=CONFIG_MONGO_SHOCK_PWD)
    else:
        client_src = MongoClient(CONFIG_MONGO_SHOCK_HOST)

    db_src = client_src[CONFIG_MONGO_SHOCK_DATABASE]

    if (args.nodemode == 'ws'):
        query = { 'acl.owner': { '$eq': CONFIG_SHOCK_WS_UUID }, 'created_on': { '$gt': CONFIG_START_DATE, '$lt': CONFIG_END_DATE } }
    elif (args.nodemode == 'shock'):
        query = { 'acl.owner': { '$ne': CONFIG_SHOCK_WS_UUID }, 'created_on': { '$gt': CONFIG_START_DATE, '$lt': CONFIG_END_DATE } }
    else:
        query = { 'created_on': { '$gt': CONFIG_START_DATE, '$lt': CONFIG_END_DATE } }

    #    print(query)
    for node in db_src[COLLECTION_SHOCK].find(query,batch_size=10000,no_cursor_timeout=True):
        #print(node['id'])
        print (node['id'][0:2] + '/' + node['id'][2:4] + '/' + node['id'][4:6] + '/' + node['id'] + '/' + node['id'] + '.data')


if __name__ == '__main__':
    main()
