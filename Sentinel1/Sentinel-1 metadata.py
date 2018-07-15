#! /usr/bin/env python
# coding:utf-8
# connect to the API
import sys
import time
import csv
import os
import psycopg2
import datetime
import logging
from sentinelsat.sentinel import SentinelAPI


def connect_db(csvfile):
    # 数据库配置参数
    db = "sentinel"
    usr = "postgres"
    pw = "postgres"
    host = "localhost"
    port = "5432"
    tb = "S1"
    # 设置log
    # logging.basicConfig(level=logging.DEBUG,
    #                     format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
    #                     datefmt='%Y %H:%M:%S',
    #                     filename='Sentineel-1.log',
    #                     filemode='w')
    print datetime.datetime.now(), os.path.basename(csvfile), "开始更新更新数据库..."
    # logging.info(str(datetime.datetime.now())+' '+os.path.basename(csvfile)+' '+"开始更新更新数据库...")
    # 连接数据库
    pgisCon = psycopg2.connect(database=db, user=usr, password=pw, host=host, port=port)
    pgisCursor = pgisCon.cursor()

    # #创建数据表字段
    creField = '''create table IF NOT EXISTS  %s(
    id VARCHAR,
    geom GEOMETRY,
    title VARCHAR,
    Product_type VARCHAR,
    Ingestion_date DATE,
    Sensing_start DATE,
    Polarisation VARCHAR,
    Mode VARCHAR,
    Status VARCHAR,
    Satellite VARCHAR,
    Instrument_abbreviation VARCHAR,
    Product_level VARCHAR,
    Instrument_name VARCHAR,
    footprint VARCHAR,
    md5 VARCHAR,
    Instrument VARCHAR,
    file_size VARCHAR)''' % (tb)
    # 执行创建表及创建字段sql语句
    pgisCursor.execute(creField)
    pgisCon.commit()
    csv_metas = get_csv_odata(csvfile)   # csv_metas是列表类型，列表项是字典
    for odata in csv_metas:
        id = odata["id"]
        title = odata["title"]
        Product_type = odata["Product_type"]
        Ingestion_date = odata["Ingestion_date"]
        Sensing_start = odata["Sensing_start"]
        Polarisation = odata["Polarisation"]
        Mode = odata["Mode"]
        Status = odata["Status"]
        Satellite = odata["Satellite"]
        Instrument_abbreviation = odata["Instrument_abbreviation"]
        Product_level = odata["Product_level"]
        Instrument_name = odata["Instrument_name"]
        footprint = odata["footprint"]
        md5 = odata["md5"]
        Instrument = odata["Instrument"]
        # url = odata["url"]
        file_size = odata["file_size"]

        istr = """insert into %s(id,geom,title,Product_type,Ingestion_date,Sensing_start,Polarisation,Mode,Status,Satellite,Instrument_abbreviation,Product_level,Instrument_name,footprint,md5,Instrument,file_size) values('%s',st_geomfromtext('%s', 4326),'%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')""" % (tb, id, footprint, title,Product_type,Ingestion_date,Sensing_start,Polarisation,Mode,Status,Satellite,Instrument_abbreviation,Product_level,Instrument_name,footprint,md5,Instrument,file_size)
        pgisCursor.execute(istr)
    pgisCon.commit()
    pgisCon.close()
    sf = open('readed.txt', 'a+')
    sf.write(os.path.basename(csvfile)+"\n")
    sf.close()
    print datetime.datetime.now(), os.path.basename(csvfile), "结束更新更新数据库..."
    # logging.info(str(datetime.datetime.now()) + ' ' + os.path.basename(csvfile) + ' ' + "结束更新更新数据库...")


def pid_meta(pid):
    id_meta = {}
    pid = pid
    meta = api.get_product_odata(id=pid,full=True)
    id = meta["id"]
    id_meta["id"] = id
    title = meta["title"]
    id_meta["title"] = title
    Product_type = meta["Product type"]
    id_meta["Product_type"] = Product_type
    Ingestion_date = meta["Ingestion Date"]
    id_meta["Ingestion_date"] = str(Ingestion_date).split(' ')[0]
    Sensing_start = meta["Sensing start"]
    id_meta["Sensing_start"] = str(Sensing_start).split(' ')[0]
    Polarisation = meta["Polarisation"]
    id_meta["Polarisation"] = Polarisation
    Mode = meta["Mode"]
    id_meta["Mode"] = Mode
    Status = meta["Status"]
    id_meta["Status"] = Status
    Satellite = meta["Satellite"]
    id_meta["Satellite"] = Satellite
    Instrument_abbreviation = meta["Instrument abbreviation"]
    id_meta["Instrument_abbreviation"] = Instrument_abbreviation
    Product_level = meta["Product level"]
    id_meta["Product_level"] = Product_level
    Instrument_name = meta["Instrument name"]
    id_meta["Instrument_name"] = Instrument_name
    footprint = meta["footprint"]
    id_meta["footprint"] = footprint
    md5 = meta["md5"]
    id_meta["md5"] = md5
    Instrument = meta["Instrument"]
    id_meta["Instrument"] = Instrument
    url = meta["url"]
    id_meta["url"] = url
    file_size = meta["Size"]
    id_meta["file_size"] = file_size
    return id_meta


def get_csv_odata(csvfile):
    meta_list = []
    f_len = open(csvfile, 'r')
    csv_len = len(f_len.readlines())-1
    f_len.close()
    f = open(csvfile,'r')
    reader = csv.DictReader(f)
    i = 1
    for row in reader:
        id = row['Id']
        # print "%d / %d"%(i,csv_len), id
        sys.stdout.write('{}/{}\r'.format(i,csv_len))
        sys.stdout.flush()
        i = i + 1
        pid_metadic = pid_meta(pid=id)
        # pid_metadic是字典类型，meta_list是列表类型
        meta_list.append(pid_metadic)
    f.close()
    return meta_list


api = SentinelAPI('chenjinlv3', 'cjl19890710', 'https://scihub.copernicus.eu/dhus')
root = "/home/tq/Sentinel/"
rf = open("readed.txt", 'r')
lines = rf.readlines()
# print lines
for fpath, dirs, fs in os.walk(root):
    for f in fs:
        csvf = os.path.join(fpath, f)

        if os.path.basename(csvf)+"\n" not in lines:
            print os.path.basename(csvf)
            connect_db(csvf)
        else:
            print os.path.basename(csvf)+"\n", "  Already exit."

