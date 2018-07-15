#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import psycopg2
import uuid
import os
import gzip
import datetime
from homura import download


if os.path.exists('./index.csv.gz'):
    os.remove('./index.csv.gz')
if os.path.exists('./index.csv'):
    os.remove('./index.csv')


# download index file to current dirctory
landsat_index_url = 'http://storage.googleapis.com/gcp-public-data-landsat/index.csv.gz'
download(landsat_index_url, './')
g = gzip.GzipFile(mode='rb', fileobj=open('./index.csv.gz', 'rb'))
f_csv = open('./index.csv', 'wb')
f_csv.write(g.read())
f_csv.close()

# config
csv_file = './index.csv'
db = 'landsat'
usr = 'postgres'
pw = 'postgres'
host = 'localhost'
port = "5432"


def update_landsat(tb):
    pgiscon = psycopg2.connect(database=db, user=usr, password=pw, host=host, port=port)
    pgiscursor = pgiscon.cursor()

    # 查询获取数据库中最新的data_acquired字段时间
    query = '''SELECT date_acquired from %s ORDER BY date_acquired DESC''' % tb
    pgiscursor.execute(query)
    acquiredtime = pgiscursor.fetchone()
    lastTime = datetime.datetime.strptime(str(acquiredtime[0]), '%Y-%m-%d')


    csvf = open(csv_file, 'r')
    lines = csvf.readlines()[1:]
    insertList = []
    for line in lines:
        strtime = line.split(',')[4]
        st = datetime.datetime.strptime(strtime, '%Y-%m-%d')
        if st >= lastTime and line.split(',')[2] == tb.upper():
            print(line)
            linelist = line.split(',')
            insertList.append(linelist)
    csvf.close()


    for insert in insertList:
        sceneid = insert[0]
        insertQuery ='''SELECT scene_id from %s WHERE scene_id = '%s' ''' % (tb, sceneid)
        pgiscursor.execute(insertQuery)
        data = pgiscursor.fetchall()
        if len(data) == 0:
            SCENE_ID = insert[0]
            PRODUCT_ID = insert[1]
            tb = insert[2]
            DATE_ACQUIRED = insert[4]
            CLOUD_COVER = float(insert[11])
            NORTH_LAT = float(insert[12])
            SOUTH_LAT = float(insert[13])
            WEST_LON = float(insert[14])
            EAST_LON = float(insert[15])
            BASE_URL = 'http://storage.googleapis.com' + insert[17].split('gs:/')[1].strip()
            uidstr = "'" + str(uuid.uuid1()) + "'"
            poly = "'" + 'POLYGON((%f %f,%f %f,%f %f,%f %f,%f %f))' % \
                         (WEST_LON, NORTH_LAT, WEST_LON, SOUTH_LAT, EAST_LON, SOUTH_LAT, EAST_LON,
                          NORTH_LAT, WEST_LON, NORTH_LAT) + "'"
            istr = """insert into  %s (uid, geom, SCENE_ID, PRODUCT_ID, DATE_ACQUIRED, CLOUD_COVER,
NORTH_LAT, SOUTH_LAT, WEST_LON, EAST_LON, BASE_URL)
values(%s, st_geomfromtext(%s, 4326),'%s', '%s', '%s', '%s', %f, %f, %f, %f, '%s')""" % \
                   (tb, uidstr, poly, SCENE_ID, PRODUCT_ID, DATE_ACQUIRED, CLOUD_COVER,
                    NORTH_LAT, SOUTH_LAT, WEST_LON, EAST_LON, BASE_URL)
            pgiscursor.execute(istr)
    pgiscon.commit()

    pgiscon.close()


landlist = ['landsat_1', 'landsat_2', 'landsat_3', 'landsat_4', 'landsat_5', 'landsat_7', 'landsat_8']
for landsat in landlist:
    update_landsat(landsat)
