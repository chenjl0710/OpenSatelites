# -*- coding: utf-8 -*-


import gzip
import csv
import psycopg2
import uuid
from homura import download

# download index file to current dirctory
sentinel_index_url = 'http://storage.googleapis.com/gcp-public-data-sentinel-2/index.csv.gz'
download(sentinel_index_url, './')
g = gzip.GzipFile(mode='rb', fileobj=open('./index.csv.gz', 'rb'))
f_csv = open('./index.csv', 'wb')
f_csv.write(g.read())
f_csv.close()
print('unzip successfully!')


# config
csv_file = './index.csv'
db = 'sentinel'
usr = 'postgres'
pw = 'postgres'
host = '127.0.0.1'
port = '5432'
tb = 's2a'

# connect to database
pgiscon = psycopg2.connect(database=db, user=usr, password=pw, host=host, port=port)

pgiscursor = pgiscon.cursor()
# create table
create_table = '''create table IF NOT EXISTS  %s(uid VARCHAR PRIMARY KEY, 
geom GEOMETRY,
GRANULE_ID VARCHAR,
PRODUCT_ID VARCHAR,
SENSING_TIME DATE,
CLOUD_COVER REAL ,
QUALITY_FLAG VARCHAR,
NORTH_LAT REAL ,
SOUTH_LAT REAL,
WEST_LON REAL,
EAST_LON REAL,
BASE_URL VARCHAR)''' % tb
# 执行创建表及创建字段sql语句
pgiscursor.execute(create_table)
pgiscon.commit()
print('create table successfully!')

# 打开csv文件，按行读取数据并记录到数据库
f = open(csv_file)
csv_reader = csv.reader(f)
next(csv_reader)
for row in csv_reader:
    GRANULE_ID = row[0]
    PRODUCT_ID = row[1]
    SENSING_TIME = row[4][:10]
    TOTAL_SIZE = 0
    CLOUD_COVER = float(row[6])
    QUALITY_FLAG = row[7]
    NORTH_LAT = float(row[9])
    SOUTH_LAT = float(row[10])
    WEST_LON = float(row[11])
    EAST_LON = float(row[12])
    BASE_URL = 'http://storage.googleapis.com' + row[13].split('gs:/')[1]

    # wkt
    poly = "'" + 'POLYGON((%f %f,%f %f,%f %f,%f %f,%f %f))' % \
                 (WEST_LON, NORTH_LAT, WEST_LON, SOUTH_LAT, EAST_LON, SOUTH_LAT, EAST_LON,
                  NORTH_LAT, WEST_LON, NORTH_LAT) + "'"
    uidstr = "'" + str(uuid.uuid1()) + "'"
    # 向数据库插入创建的geometry及属性
    istr = """insert into  %s (uid, geom, GRANULE_ID, PRODUCT_ID, SENSING_TIME, CLOUD_COVER,
QUALITY_FLAG, NORTH_LAT,SOUTH_LAT,WEST_LON,EAST_LON,BASE_URL) 
    values(%s, st_geomfromtext(%s, 4326),'%s', '%s', '%s', '%s', '%s', %f, %f, %f, %f, '%s')"""\
           % (tb, uidstr, poly, GRANULE_ID, PRODUCT_ID, SENSING_TIME, CLOUD_COVER, QUALITY_FLAG,
              NORTH_LAT, SOUTH_LAT, WEST_LON, EAST_LON, BASE_URL)
    pgiscursor.execute(istr)
    pgiscon.commit()
f.close()
print('all data have be inserted!')

# 创建分区表空间索引
pgiscursor.execute('''create index %s_geo_index on %s using gist(geom)''' % (tb, tb))
print("空间索引创建完毕...")
pgiscon.commit()
pgiscursor.close()
pgiscon.close()
