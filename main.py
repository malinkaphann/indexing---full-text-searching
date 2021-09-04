import os
import sys
import json
import psycopg2
from pprint import pprint
    
def getDb():

    # read from environment variables
    host = os.environ['PG_HOST']
    user = os.environ['PG_USER']
    pswd = os.environ['PG_PASS']
    db   = os.environ['PG_DB']

    # connect to database
    try:
        conn = psycopg2.connect('host={} dbname={} user={} password={}'.format(
        host, db, user, pswd
        ))
    except:
        print('error connecting to database')
        sys.exit()

    # get its cursor to perform the operations
    cursor = conn.cursor()

    # return both conn and cursor
    return {
        "conn": conn,
        "cursor": cursor
    }


def buildOrString(list):

    result = ""

    for idx, val in enumerate(list):
        if idx < len(list) - 1:
            result = "{} || ' ' || {}".format(val['column'], result);
        else:
            result = "{}{}".format(result, val['column'])

    return result


def createIndexes(cursor, conn):

    # try to open config file
    try:
        with open('config.json', 'r') as f:
            config = json.load(f)
    except:
        print('error opening config.json')
        sys.exit()
    
    # find key table
    try:
        myTable = config['table']
    except:
        print('key table is not found in config')
        sys.exit()

    # find key indexes
    try:
        indexes = config['indexes']
    except:
        print('key indexes is not found in config')
        sys.exit()

    # find key fts_index
    try:
        fts_index = config['fts_index']
    except:
        print('key fts_index is not found in config')
        sys.exit()

    print('1- working with normal column indexes')

    # loop through indexes
    for index in indexes:

        print('drop index {}'.format(index['index']))
        cursor.execute('DROP INDEX IF EXISTS {}'.format(index['index']))
        print('create index {} using btree'.format(index['index']))
        cursor.execute('CREATE INDEX {} ON {} USING BTREE ({});'.format(index['index'], 
            myTable, index['column']))

    # for full text search
    
    print('2- working with column index for full text search')
    print('drop column document_idx')
    cursor.execute('ALTER TABLE {} DROP COLUMN IF EXISTS document_idx'.format(myTable))
    print('adding column document_idx')
    cursor.execute('ALTER TABLE {} ADD COLUMN document_idx tsvector'.format(myTable))

    print('update document_idx')
    tsValue = buildOrString(indexes)
    cursor.execute("UPDATE {} SET document_idx = to_tsvector ({});".format(myTable, tsValue))

    print('drop index document_with_idx')
    cursor.execute('DROP INDEX IF EXISTS document_with_idx')
    print('create index document_with_idx using gin')
    cursor.execute('CREATE INDEX document_with_idx ON {} USING GIN (document_idx);'.format(myTable))

    conn.commit()

if __name__ == "__main__":
    
    db = getDb()
    createIndexes(db['cursor'], db['conn'])
