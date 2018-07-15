# -*- coding: utf-8 -*-
import csv
import os
import psycopg2
import datetime


def insert2pg(csvRowList, tb):
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

    # insrtStr = "insert into {0} values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)".format(tb)
    insrtStr = "insert into {0} values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s,%s,%s,%s,%s,st_geomfromtext(%s,4326))".format(tb)
    pgiscursor.executemany(insrtStr, csvRowList)
    pgiscon.commit()
    print('import {} data successfully'.format(tb))
    # 创建分区表空间索引
    # pgiscursor.execute("create index {0} on {1} using gist(geom)".format(tb+"_geo_index", tb))
    # pgiscursor.close()
    # pgiscon.commit()
    pgiscon.close()


def xiJ(xj): #lon
    if xj <= 0:
        nxj = 360 + xj
    else:
        nxj = xj
    return nxj


def weiD(wd): #lat
    if wd <= 0:
        nwd = -wd
    else:
        nwd = wd
    return nwd


def csv2db(csv_file, tb):
    ocf = open(csv_file)
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
            # clat = weiD(float(row[13]))
            # clon = xiJ(float(row[14]))
            # nslat = weiD(float(row[15]))
            # nslon = xiJ(float(row[16]))
            # fslat = weiD(float(row[17]))
            # fslon = xiJ(float(row[18]))
            # nelat = weiD(float(row[19]))
            # nelon = xiJ(float(row[20]))
            # felat = weiD(float(row[21]))
            # felon = xiJ(float(row[22]))
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
        # tb_name_n = tb + '_' + os.path.basename(csv_file).split('-')[-2]
        # print(tb_name_n)
        insert2pg(csvRowList=csv_RowList, tb=tb)
        print(str(datetime.datetime.now()) + os.path.basename(csv_file)+"导入完成。")
    except:
        print(str(datetime.datetime.now()) + os.path.basename(csv_file)+">>>>>>>>>>>>>>>>>是一个空csv")
    ocf.close()


if __name__ == '__main__':
    db = 'SAR'
    usr = 'postgres'
    pw = 'postgres'
    host = '10.16.20.27'
    port = '5432'
    rootdir = "/home/tq/Codes/SAR/meta"
    for root, dirs, files in os.walk(rootdir):
        for p_dir in dirs:
            print(p_dir)
            tb = p_dir.replace('-', '_')
            p = os.path.join(root, p_dir)
            print(">"*60)
            print("p:", p)
            for f_path, ds, fs in os.walk(p):
                for f in fs:
                    w_file = os.path.join(f_path, f)
                    if w_file.endswith(".csv"):
                        print("w_file:", w_file)
                        tb_name = tb# + '_' + os.path.basename(w_file).split('-')[-2]
                        csv2db(csv_file=w_file, tb=tb)
                        print('<'*60)
