# -*- coding: utf-8 -*-
import os
import csv
import datetime
import psycopg2

def insert2pg(csvRowList,tb):
    # connect to database
    pgiscon = psycopg2.connect(database=db, user=usr, password=pw, host=host, port=port)
    pgiscursor = pgiscon.cursor()
    create_table = '''create table IF NOT EXISTS  %s(Granule_Name VARCHAR(100),
        Platform VARCHAR,
        Sensor VARCHAR,
        Beam_Mode VARCHAR,
        Beam_Mode_Description VARCHAR,
        Orbit VARCHAR,
        Path_Number VARCHAR,
        Frame_Number VARCHAR,
        Acquisition_Date DATE,
        Processing_Level VARCHAR,
        Center_Lat REAL,
        Center_Lon REAL,
        Near_Start_Lat REAL,
        Near_Start_Lon REAL,
        Far_Start_Lat REAL,
        Far_Start_Lon REAL,
        Near_End_Lat REAL,
        Near_End_Lon REAL,
        Far_End_Lat REAL,
        Far_End_Lon REAL,
        Faraday_Rotation VARCHAR,
        URL VARCHAR,
        Size_MB REAL,
        Off_Nadir_Angle VARCHAR,
        Stack_Size VARCHAR,
        Baseline_Perp VARCHAR,
        Doppler VARCHAR,
        geom GEOMETRY)''' % tb
    pgiscursor.execute(create_table)
    pgiscon.commit()

    insrtStr = "insert into {0} values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s,%s,%s,%s,%s,st_geomfromtext(%s,4326))".format(tb)
    pgiscursor.executemany(insrtStr, csvRowList)
    pgiscon.commit()
    print('import {} data successfully'.format(tb))
    pgiscon.close()

def csv2db(csv_f,tb):
    ocf = open(csv_f)
    csv_reader = csv.reader(ocf)
    try:
        next(csv_reader)
        csv_RowList = []
        for row in csv_reader:
            name_r = row[0]
            pltform = row[1]
            snsor = row[2]
            beanmode = row[3]
            beanmode_des = row[4]
            obit = row[5]
            pnum = row[6]
            fnum = row[7]
            acquiretime = row[8]
            prcs_lelv = row[10]
            clat = float(row[13])
            clon = float(row[14])
            nslat = float(row[15])
            nslon = float(row[16])
            fslat = float(row[17])
            fslon = float(row[18])
            nelat = float(row[19])
            nelon = float(row[20])
            felat = float(row[21])
            felon = float(row[22])
            rotation = row[23]
            url = row[25]
            size = float(row[26])
            angle = row[27]
            stacksize = row[28]
            baseline = row[29]
            dop = row[30]
            poly = 'POLYGON(({0} {1},{2} {3},{4} {5},{6} {7},{8} {9}))'.format(nslon, nslat, fslon, fslat, felon, felat,
                                                                               nelon, nelat, nslon, nslat)
            row_tuple = (name_r, pltform, snsor, beanmode, beanmode_des, obit, pnum, fnum, acquiretime, prcs_lelv, clat,
                         clon, nslat, nslon, fslat, fslon, nelat, nelon, felat, felon, rotation, url, size, angle,
                         stacksize, baseline, dop, poly)
            csv_RowList.append(row_tuple)
        insert2pg(csvRowList=csv_RowList, tb=tb)
        print(str(datetime.datetime.now()) + os.path.basename(csv_f)+"导入完成。")
    except Exception, e:
        print(str(datetime.datetime.now()) + os.path.basename(csv_f)+"是一个空csv")
    ocf.close()

def metaTimeDownload(csv_folder):
    # platform = "Sentinel-1A"
    # processingLevel = "GRD_HS,GRD_HD,GRD_MS,GRD_MD,GRD_FS,GRD_FD,SLC,RAW,OCN"
    platform_processingLevel = {
        "Sentinel-1A":"GRD_HS,GRD_HD,GRD_MS,GRD_MD,GRD_FS,GRD_FD,SLC,RAW,OCN",
        "Sentinel-1B":"GRD_HS,GRD_HD,GRD_MS,GRD_MD,GRD_FS,GRD_FD,SLC,RAW,OCN",
        "SMAP":"L1A_Radar_RO_QA,L1B_S0_LoRes_HDF5,L1B_S0_LoRes_QA,L1B_S0_LoRes_ISO_XML,L1A_Radar_QA,L1A_Radar_RO_ISO_XML,L1C_S0_HiRes_ISO_XML,L1C_S0_HiRes_QA,L1C_S0_HiRes_HDF5,L1A_Radar_HDF5",
        "UAVSAR":"PROJECTED,PAULI,PROJECTED_ML5X5,STOKES,AMPLITUDE,COMPLEX,DEM_TIFF,PROJECTED_ML3X3,AMPLITUDE_GRD,INTERFEROMETRY,INTERFEROMETRY_GRD"
    }
    for i, key in enumerate(platform_processingLevel):
        platform = key
        processingLevel = platform_processingLevel[key]
        maxResults = "1000000"

        dtTime = metaTimeUpdate(tb=platform.replace('-','_'))
        dt_start = dtTime + "T00:00:00UTC"
        yesterday = datetime.datetime.today() + datetime.timedelta(-1)

        dt_end = yesterday.strftime('%Y-%m-%d') + "T11:59:59UTC"
        csv_file = os.path.join(csv_folder, platform + "-" + dt_end[:10] + ".csv")
        # print(csv_file)
        print(platform,dt_start,dt_end,csv_file)
        httpRequest = '''curl "https://api.daac.asf.alaska.edu/services/search/param?platform={0}&processingLevel={1}\
&start={2}&end={3}&maxResults={4}&output=csv" > {5}'''.format(platform, processingLevel, dt_start, dt_end,maxResults, csv_file)
        # print(httpRequest)

        os.system(httpRequest)
        csv2db(csv_file,platform.replace('-','_'))

def metaTimeUpdate(tb):
    #定时主动下载元信息，保存格式csv
    pgiscon = psycopg2.connect(database=db, user=usr, password=pw, host=host, port=port)
    pgiscursor = pgiscon.cursor()

    # 查询获取数据库中最新的sensing_time字段时间
    query = '''SELECT acquisition_date from %s ORDER BY acquisition_date DESC Limit 1''' % tb
    pgiscursor.execute(query)
    senseTime = pgiscursor.fetchone()
    # lastTime = datetime.datetime.strptime(str(senseTime[0]), '%Y-%m-%d')#.split(" ")[0]
    lastTime = str(senseTime[0])
    return lastTime

if __name__ == '__main__':
    #定时主动下载元信息，并入库
    db = 'SAR'
    usr = 'postgres'
    pw = 'postgres'
    host = '10.16.20.27'
    port = "5432"
    csv_folder = "./meta/update"
    metaTimeDownload(csv_folder)