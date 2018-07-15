# -*- coding: utf-8 -*-
import datetime
import os
import csv


def metaPositiveDownload(platforms,processingLevel,start_year,end_year,csv_folder,maxResults):
    #元信息下载，保存格式csv
    if not os.path.exists(csv_folder):
        os.mkdir(csv_folder)
    for y in range(start_year, end_year+1):
        for x in range(1, 13):
            dt_start = (datetime.datetime(y, x, 1)).strftime("%Y-%m-%d") + "T00:00:00UTC"
            if 12 == x:
                dt_end = (datetime.datetime(y, 12, 31)).strftime("%Y-%m-%d") + "T11:59:59UTC"
            else:
                dt_end = (datetime.datetime(y, x + 1, 1) - datetime.timedelta(days=1)).strftime(
                    "%Y-%m-%d") + "T11:59:59UTC"
            csv_file = os.path.join(csv_folder, platform + "-" + dt_start[:7] + ".csv")
            if not os.path.exists(csv_file):
                print(str(datetime.datetime.now()),csv_file)
                httpRequest = '''curl "https://api.daac.asf.alaska.edu/services/search/param?\
platform={0}&processingLevel={1}\
&start={2}&end={3}&maxResults={4}&output=csv" > {5}'''.format(platform, processingLevel, dt_start, dt_end,maxResults, csv_file)
                print(httpRequest)
                os.system(httpRequest)


if __name__ == '__main__':
    sar_data = 'sar_Data.csv'
    f = open(sar_data)
    csv_reader = csv.reader(f,delimiter = ';')
    root = "./meta"
    maxResults = "1000000000"
    if not os.path.exists(root):
        os.mkdir(root)
    try:
        next(csv_reader)
        for sat in csv_reader:
            print sat
            platform = sat[0]
            processingLevel = sat[1]
            start_year = int(sat[2])
            end_year = int(sat[3])
            print "print:",platform,processingLevel,start_year,end_year
            csv_folder = os.path.join(root,platform)
            try:
                os.mkdir(csv_folder)
            except:
                pass
            metaPositiveDownload(platform,processingLevel,start_year,end_year,csv_folder,maxResults)

    except:
        pass
