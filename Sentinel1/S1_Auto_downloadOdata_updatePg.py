#! /usr/bin/env python
# coding:utf-8

import requests,time,datetime
import os,json
import psycopg2

def get_mata(platformname,ingestion_enddate,rootpath,filetype = "json"):
    s = requests.session()
    s.auth = ('chenjinlv3','cjl19890710')
    s.headers.update({'x-test': 'true'})
    i = 0
    start = 0
    url = 'https://scihub.copernicus.eu/dhus/search?q=platformname:{0} AND beginposition:[{1}]&start={2}&rows=100&format={3}&orderby=beginposition asc'\
        .format(platformname,ingestion_enddate,start,filetype)
    print url
    r = s.get(url)
    print r.status_code
    # print r.content
    csv_time = datetime.datetime.now().strftime("%Y %m %d %H %M %S").replace(' ','')
    sv_filename = csv_time + '_' +platformname + '_' + str(start) + '_' + str(start+99)  + '.' + filetype
    xml_f = os.path.join(rootpath,sv_filename)
    if os.path.exists(xml_f):
        pass
    else:
        f = open(xml_f,'w')
        f.write(r.content)
        f.close()

    dicts = json.loads(r.content)
    total = int(dicts["feed"]["opensearch:totalResults"])

    while total >= start:
        start = start + 100
        sv_filename = csv_time + '_' +platformname + '_' + str(start) + '_' + str(start+99)  + '.' + filetype
        xml_file = os.path.join(rootpath,sv_filename)
        if os.path.exists(xml_file):
            print 'Alread exits ',time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())),u'【',csv_time,platformname,u'】',\
                str(round(float(start + 99)/float(total)*100,4))+'%-', str(start) + '_' + str(start + 99) + '  /  ' + str(total)
        else:
            url = 'https://scihub.copernicus.eu/dhus/search?q=platformname:{0} AND beginposition:[{1}]&start={2}&rows=100&format={3}&orderby=beginposition asc'\
                .format(platformname,ingestion_enddate,start,filetype)
            r = s.get(url)
            print r.status_code, '-',time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())),u'【',csv_time,platformname,u'】',\
                str(round(float(start + 99)/float(total)*100,4))+'%-', str(start) + '_' + str(start + 99) + '  /  ' + str(total)
            f = open(xml_file,'w')
            f.write(r.content)
            f.close()
            odata2pg(xml_file)
            print(sv_filename+" has been imported into db.")

def getDbRecentDate():
    # 数据库配置参数
    db = database
    usr = user
    pw = password
    hst = host
    pt = port
    tb = table
    print tb
    # 连接数据库
    pgisCon = psycopg2.connect(database=db, user=usr, password=pw, host=hst, port=pt)
    pgisCursor = pgisCon.cursor()
    sql_search = """select beginposition from {} order by beginposition Desc limit 1""".format(tb)
    pgisCursor.execute(sql_search)
    rows = pgisCursor.fetchall()
    newest_date = (rows[0][0]+datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    print newest_date
    pgisCon.close()
    return newest_date

def odata2pg(js_file):
    # 数据库配置参数
    db = database
    usr = user
    pw = password
    hst = host
    pt = port
    tb = table
    # 连接数据库
    pgisCon = psycopg2.connect(database=db, user=usr, password=pw, host=hst, port=pt)
    pgisCursor = pgisCon.cursor()

    jsf = open(js_file, 'r')
    jsonstr = jsf.readline()
    dicts = json.loads(jsonstr)
    if dicts["feed"].has_key("entry"):
        entrys = dicts["feed"]["entry"]
        for index, item in enumerate(entrys):
            dates = item["date"]
            for dt in dates:
                if dt["name"] == "beginposition":
                    beginposition_ = dt["content"].split('T')[0]
            strs = item["str"]
            swathidentifier_N=""
            for item_str in strs:
                if item_str["name"] == "filename":
                    filename_ = item_str["content"]

                if item_str["name"] == "acquisitiontype":
                    acquisitiontype_ = item_str["content"]

                if item_str["name"] == "instrumentshortname":
                    instrumentshortname_ = item_str["content"]

                if item_str["name"] == "sensoroperationalmode":
                    sensoroperationalmode_ = item_str["content"]

                if item_str["name"] == "instrumentname":
                    instrumentname_ = item_str["content"]

                if item_str["name"] == "swathidentifier":
                    swathidentifier_N = item_str["content"]

                if item_str["name"] == "footprint":
                    footprint_ = item_str["content"].replace("POLYGON ", "POLYGON")

                if item_str["name"] == "polarisationmode":
                    polarisationmode_ = item_str["content"]

                if item_str["name"] == "productclass":
                    productclass_ = item_str["content"]

                if item_str["name"] == "producttype":
                    producttype_ = item_str["content"]

                if item_str["name"] == "platformname":
                    platformname_ = item_str["content"]

                if item_str["name"] == "size":
                    size_ = item_str["content"]

                if item_str["name"] == "status":
                    status_ = item_str["content"]

                if item_str["name"] == "uuid":
                    uuid_ = item_str["content"]
                    downloadurl_ = """https://scihub.copernicus.eu/dhus/odata/v1/Products({0})/$value""".format(uuid_)
                    quicklook_ = """https://scihub.copernicus.eu/dhus/odata/v1/Products({0})/Products(Quicklook)/$value""".format(uuid_)
            in_str = """insert into %s (beginposition, filename, acquisitiontype, instrumentshortname, sensoroperationalmode, instrumentname, swathidentifier, footprint, polarisationmode, productclass, producttype, platformname, size, status, uuid, downloadurl, quicklook, geom) values ('%s', '%s','%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', st_geomfromtext('%s',4326))""" % (
                tb, beginposition_, filename_, acquisitiontype_, instrumentshortname_, sensoroperationalmode_,
                instrumentname_, swathidentifier_N, footprint_, polarisationmode_, productclass_, producttype_,
                platformname_, size_, status_, uuid_, downloadurl_, quicklook_, footprint_)
            pgisCursor.execute(in_str)
        # 将插入的数据提交至数据库
    pgisCon.commit()
    jsf.close()
    pgisCon.close()


global table,database,user,password,host,port
database = "sentinel"
table = "s1"
user = "postgres"
password = "postgres"
host = "localhost"
port = "5432"

rootpath = './odata'
platformname = 'Sentinel-1'
db_newestDate = getDbRecentDate()
nowDate = time.strftime('%Y-%m-%d', time.localtime(time.time()))
ingestion_enddate = db_newestDate + "T00:00:00.000Z TO "+ nowDate + "T00:00:00.000Z"
print(platformname,"--",ingestion_enddate)
get_mata(platformname,ingestion_enddate,rootpath)
