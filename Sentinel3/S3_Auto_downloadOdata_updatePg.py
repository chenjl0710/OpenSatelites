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
    # year = str(ingestion_enddate[0:4])
    csv_time = datetime.datetime.now().strftime("%Y %m %d %H %M %S").replace(' ', '')
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
            row = []
            dates = item["date"]
            for dt in dates:
                if dt["name"] == "beginposition":
                    beginposition = dt["content"].split('T')[0]

            strs = item["str"]
            for item_str in strs:
                if item_str["name"] == "filename":
                    filename = item_str["content"]

                if item_str["name"] == "instrumentshortname":
                    instrumentshortname = item_str["content"]

                if item_str["name"] == "sensoroperationalmode":
                    sensoroperationalmode = item_str["content"]

                if item_str["name"] == "instrumentname":
                    instrumentname = item_str["content"]

                if item_str["name"] == "procfacilityname":
                    procfacilityname = item_str["content"]

                if item_str["name"] == "procfacilityorg":
                    procfacilityorg = item_str["content"]

                if item_str["name"] == "processinglevel":
                    processinglevel = item_str["content"]

                if item_str["name"] == "footprint":
                    footprint = item_str["content"].replace("POLYGON ", "POLYGON")

                if item_str["name"] == "onlinequalitycheck":
                    onlinequalitycheck = item_str["content"]

                if item_str["name"] == "productlevel":
                    productlevel = item_str["content"]

                if item_str["name"] == "producttype":
                    producttype = item_str["content"]

                if item_str["name"] == "platformname":
                    platformname = item_str["content"]

                if item_str["name"] == "size":
                    size = item_str["content"]

                if item_str["name"] == "mode":
                    mode = item_str["content"]

                if item_str["name"] == "uuid":
                    uuid = item_str["content"]
                    downloadurl = "https://scihub.copernicus.eu/dhus/odata/v1/Products(" + uuid + ")/$value"
                    quicklook = "https://scihub.copernicus.eu/dhus/odata/v1/Products(" + uuid + ")/Products(Quicklook)/$value"
            in_str = """insert into %s (beginposition,filename,instrumentshortname,sensoroperationalmode,instrumentname,procfacilityname,procfacilityorg,processinglevel,footprint,onlinequalitycheck,productlevel,producttype,platformname,size,mode,uuid,downloadurl,quicklook,geom) values ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s', st_geomfromtext('%s',4326))""" % (
                tb, beginposition, filename, instrumentshortname, sensoroperationalmode, instrumentname,
                procfacilityname, procfacilityorg, processinglevel, footprint, onlinequalitycheck, productlevel,
                producttype, platformname, size, mode, uuid, downloadurl, quicklook, footprint)
            pgisCursor.execute(in_str)
    # 将插入的数据提交至数据库
    pgisCon.commit()
    jsf.close()
    pgisCon.close()

if __name__=="__main__":
    global table,database,user,password,host,port
    database = "sentinel"
    table = "s3"
    user = "postgres"
    password = "postgres"
    host = "localhost"
    port = "5432"

    rootpath = './odata'
    platformname = 'Sentinel-3'

    db_newestDate = getDbRecentDate()
    nowDate = time.strftime('%Y-%m-%d', time.localtime(time.time()))
    ingestion_enddate = db_newestDate + "T00:00:00.000Z TO "+ nowDate + "T00:00:00.000Z"
    print(platformname,"--",ingestion_enddate)
    get_mata(platformname,ingestion_enddate,rootpath)
