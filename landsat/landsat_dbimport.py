# -*- coding: utf-8 -*-


import gzip
import csv
import psycopg2
import uuid
from homura import download

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
host = '127.0.0.1'
port = '5432'


def dbimport(tb):
    # connect to database
    pgiscon = psycopg2.connect(database=db, user=usr, password=pw, host=host, port=port)
    pgiscursor = pgiscon.cursor()
    # create table
    create_table = '''create table IF NOT EXISTS  %s(uid VARCHAR PRIMARY KEY,
geom GEOMETRY,
SCENE_ID VARCHAR,
PRODUCT_ID VARCHAR,
DATE_ACQUIRED DATE,
CLOUD_COVER REAL ,
NORTH_LAT REAL ,
SOUTH_LAT REAL,
WEST_LON REAL,
EAST_LON REAL,
BASE_URL VARCHAR)''' % tb
# 执行创建表及创建字段sql语句
    pgiscursor.execute(create_table)
    pgiscon.commit()

    # 打开csv文件，按行读取数据并记录到数据库
    f = open(csv_file)
    csv_reader = csv.reader(f)
    next(csv_reader)
    i = 0
    j = 1
    for row in csv_reader:
        SCENE_ID = row[0]
        PRODUCT_ID = row[1]
        DATE_ACQUIRED = row[4]
        CLOUD_COVER = float(row[11])
        NORTH_LAT = float(row[12])
        SOUTH_LAT = float(row[13])
        WEST_LON = float(row[14])
        EAST_LON = float(row[15])
        BASE_URL = 'http://storage.googleapis.com' + row[17].split('gs:/')[1]

        # wkt
        poly = "'" + 'POLYGON((%f %f,%f %f,%f %f,%f %f,%f %f))' % \
                     (WEST_LON, NORTH_LAT, WEST_LON, SOUTH_LAT, EAST_LON,
                      SOUTH_LAT, EAST_LON, NORTH_LAT, WEST_LON, NORTH_LAT) + "'"
        uidstr = "'" + str(uuid.uuid1()) + "'"
        # 向数据库插入创建的geometry及属性
        istr = """insert into  %s (uid, geom, SCENE_ID, PRODUCT_ID, DATE_ACQUIRED, CLOUD_COVER,
NORTH_LAT,SOUTH_LAT,WEST_LON,EAST_LON,BASE_URL)
values(%s, st_geomfromtext(%s, 4326),'%s', '%s', '%s', '%s', %f, %f, %f, %f, '%s')""" \
               % (tb, uidstr, poly, SCENE_ID, PRODUCT_ID, DATE_ACQUIRED, CLOUD_COVER,
                  NORTH_LAT, SOUTH_LAT, WEST_LON, EAST_LON, BASE_URL)
        i = i + 1
        pgiscursor.execute(istr)
        # 每一万次向数据库提交一次数据
        if i >= 10000:
            pgiscon.commit()
            i = 0
            j = j + 1
    # 最后提交一次
    pgiscon.commit()
    f.close()
    # 创建分区表空间索引
    # pgiscursor.execute('create index %s_geo_index on %s using gist(geom)' % (tb, tb))
    # print("空间索引创建完毕...")
    # pgiscursor.close()
    # pgiscon.commit()

    pgiscon.close()


with open('./index.csv', 'r') as index_file:
    index_csv = csv.reader(index_file)
    header = next(index_csv)
    for row in index_csv:
        SPACECRAFT_ID = row[2]
        dbimport(SPACECRAFT_ID)
