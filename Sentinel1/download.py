#! /usr/bin/env python
# coding:utf-8
# connect to the API
from sentinelsat.sentinel import SentinelAPI, read_geojson, geojson_to_wkt
from datetime import date
import datetime
from osgeo import ogr
api = SentinelAPI('chenjinlv', 'cjl19890710', 'https://scihub.copernicus.eu/dhus')


# search by polygon, time, and SciHub query keywords
# footprint = geojson_to_wkt(read_geojson(r"C:\Users\Administrator\Downloads\Compressed\alibaba_crawl-master\alibaba_crawl\sentinelsat-master\tests\fixtures\map.geojson"))


def download_by_shp(shapefile,directory_path):
    shapefile = r"C:\Users\Administrator\Desktop\boundary\HuaiYuan_boundary.shp"
    driver = ogr.GetDriverByName("ESRI Shapefile")
    dataSource = driver.Open(shapefile, 0)
    layer = dataSource.GetLayer()

    for feature in layer:
        geom = feature.GetGeometryRef()
        footprint = ogr.Geometry.ExportToWkt(geom)
        print footprint
        products = api.query(footprint, date=('20170701', date(2017, 9, 30)), platformname='Sentinel-2',
                             cloudcoverpercentage=(0, 30))
        #     print type(products)
        #     for i,pro in enumerate(products):
        #         uuid = pro[0]
        #         title = pro[1]['title']
        #         print uuid,'----',title,'----',pro
        api.download_all(products, directory_path=r"C:\Users\Administrator\Desktop\boundary\huaiyuan")
def download_by_wkt(footprint,directory_path):
    # footprint = 'POLYGON((115.3780875744767 34.94230836209809,114.66419386674173 33.911843123420255,114.93319019069726 32.50447536913384,115.75416795459248 30.311252608773643,116.80716117350157 30.079867309087874,117.73522299355702 30.326658945076062,119.03807901017338 31.322840633627564,118.75252152707938 31.839777376339242,117.98508579126428 32.18783830539162,117.68401827940696 33.71011784092819,116.49622466006386 34.53419096965092,116.26420920505 35.09100413512847,115.3780875744767 34.94230836209809,115.3780875744767 34.94230836209809)))'
    print footprint
    # products = api.query(footprint, date=('20170101', date(2017, 10, 11)), platformname='Sentinel-1',roducttype='SLC',beginPosition="2017-01-01T00:00:00.000Z TO 2017-10-11T23:59:59.999Z")
    products = api.query(footprint, platformname='Sentinel-1',roducttype='SLC',beginPosition="2017-01-01T00:00:00.000Z TO 2017-10-11T23:59:59.999Z")
    print type(products)
    # for i,pro in enumerate(products):
    #     uuid = pro[0]
    #     title = pro[1]['title']
    #     print uuid,'----',title,'----',pro
    api.download_all(products, directory_path=r"C:\Users\Administrator\Desktop\boundary\huaiyuan")

footprint = 'POLYGON((115.3780875744767 34.94230836209809,114.66419386674173 33.911843123420255,114.93319019069726 32.50447536913384,115.75416795459248 30.311252608773643,116.80716117350157 30.079867309087874,117.73522299355702 30.326658945076062,119.03807901017338 31.322840633627564,118.75252152707938 31.839777376339242,117.98508579126428 32.18783830539162,117.68401827940696 33.71011784092819,116.49622466006386 34.53419096965092,116.26420920505 35.09100413512847,115.3780875744767 34.94230836209809,115.3780875744767 34.94230836209809))'
directory_path = r"D:\svn"
download_by_wkt(footprint,directory_path)
