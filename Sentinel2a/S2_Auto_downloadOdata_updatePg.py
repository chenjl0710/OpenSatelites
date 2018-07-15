#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import psycopg2
import uuid
import os
import gzip
import datetime
import time
from homura import download


if os.path.exists('./index.csv.gz'):
    os.remove('./index.csv.gz')
if os.path.exists('./index.csv'):
    os.remove('./index.csv')


# download index file to current dirctory
sentinel_index_url = 'http://storage.googleapis.com/gcp-public-data-sentinel-2/index.csv.gz'
download(sentinel_index_url, './')
g = gzip.GzipFile(mode='rb', fileobj=open('./index.csv.gz', 'rb'))
f_csv = open('./index.csv', 'wb')
f_csv.write(g.read())
f_csv.close()

# config
csv_file = './index.csv'
db = 'sentinel'
usr = 'postgres'
pw = 'postgres'
host = 'localhost'
port = "5432"
tb = 's2'


pgiscon = psycopg2.connect(database=db, user=usr, password=pw, host=host, port=port)
pgiscursor = pgiscon.cursor()

# 查询获取数据库中最新的sensing_time字段时间
query = '''SELECT sensing_time from %s ORDER BY sensing_time DESC''' % tb
pgiscursor.execute(query)
senseTime = pgiscursor.fetchone()
lastTime = datetime.datetime.strptime(str(senseTime[0]), '%Y-%m-%d')


# 逐条遍历csv获取sensing_time字段值，并与数据中sensing_time字段的最新值比较，若csv中的sensing_time比数据中的sensing_time值大，则遍历的该行数据增加到insertList列表中
csvf = open(csv_file, 'r')
lines = csvf.readlines()[1:]
insertList = []
for line in lines:
    strtime = line.split(',')[4][:10]
    st = datetime.datetime.strptime(strtime, '%Y-%m-%d')
    if st >= lastTime:
        linelist = line.split(',')
        insertList.append(linelist)
csvf.close()

for insert in insertList:
    granule_id_query = insert[0]
    insertQuery ='''SELECT granule_id from %s WHERE granule_id = '%s' ''' % (tb, granule_id_query)
    pgiscursor.execute(insertQuery)
    data = pgiscursor.fetchall()
    if len(data) == 0:
        GRANULE_ID = insert[0]
        PRODUCT_ID = insert[1]
        SENSING_TIME = insert[4][:10]
        CLOUD_COVER = float(insert[6])
        QUALITY_FLAG = insert[7]
        NORTH_LAT = float(insert[9])
        SOUTH_LAT = float(insert[10])
        WEST_LON = float(insert[11])
        EAST_LON = float(insert[12])
        BASE_URL = 'http://storage.googleapis.com' + insert[13].split('gs:/')[1].strip()
        uidstr = "'" + str(uuid.uuid1()) + "'"
        poly = "'" + 'POLYGON((%f %f,%f %f,%f %f,%f %f,%f %f))' % \
                     (WEST_LON, NORTH_LAT, WEST_LON, SOUTH_LAT, EAST_LON, SOUTH_LAT, EAST_LON,
                      NORTH_LAT, WEST_LON, NORTH_LAT) + "'"
        istr = """insert into  %s (uid, geom, GRANULE_ID, PRODUCT_ID, SENSING_TIME, CLOUD_COVER, 
QUALITY_FLAG, NORTH_LAT,SOUTH_LAT,WEST_LON,EAST_LON,BASE_URL) 
values(%s, st_geomfromtext(%s, 4326),'%s', '%s', '%s', '%s', '%s', %f, %f, %f, %f, '%s')""" % \
               (tb, uidstr, poly, GRANULE_ID, PRODUCT_ID, SENSING_TIME, CLOUD_COVER, QUALITY_FLAG,
                NORTH_LAT, SOUTH_LAT, WEST_LON, EAST_LON, BASE_URL)
        pgiscursor.execute(istr)
pgiscon.commit()

pgiscon.close()
