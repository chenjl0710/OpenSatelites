#! /usr/bin/env python
# coding:utf-8
# 本工程查询输入shapefile与数据库中相交的影像，并数据csv文件
import csv, os, psycopg2
import datetime
try:
    from osgeo import gdal, ogr, osr
except ImportError:
    import gdal, ogr, osr

startTime = datetime.datetime.now()
print "starting time:", startTime

# 输入待查询区域的shapefile文件
shp_file = '/home/tq/Documents/hexian.shp'
# 检索参数设置
cloud = float(50)
start_time = '2016-05-01'  # 格式如 2017-01-01   2017-12-12
end_time = '2017-09-03'  # 格式如 2017-01-01   2017-12-12

# 输出csv
f = open(os.path.join(os.path.dirname(shp_file), os.path.basename(shp_file).split('.shp')[0] + "_CLOUD"+str(cloud).split('.')[0] + "_"  +start_time + "_TO_" + end_time + ".txt"),'wb')
writer = csv.writer(f)
# title = "uid" + "," + "granule_id" + "," + "product_id" + "," + "datatake_identifier" + "," + "mgrs_tile" + "," + "sensing_time" + "," + "cloud_cover" + "," + "base_url" +"\n"
# title = ["product_id", "base_url"]
# print title
# writer.writerow(title)

# 输出shapefile
outshp = os.path.join(os.path.dirname(shp_file),os.path.basename(shp_file).split('.shp')[0] + "_CLOUD"+str(cloud).split('.')[0] + "_" +start_time + "_TO_" + end_time + ".shp")
print outshp

# 数据库配置参数
db = "sentinel"
usr = "postgres"
pw = "postgres"
host = "localhost"
port = "5432"
tb = "s2a"  #sentineltable

# 连接数据库
pgisCon = psycopg2.connect(database=db,user=usr,password=pw,host=host,port=port)
pgisCursor = pgisCon.cursor()

# 获取输入矢量的geometry
driver = ogr.GetDriverByName('ESRI Shapefile')
dataSource = driver.Open(shp_file,0)
if dataSource is not None:
    layer = dataSource.GetLayer(0)
    feat = layer.GetNextFeature()
    # while feat:
    geom_shp = feat.GetGeometryRef()
    # print "geom_shp:",type(geom_shp)
    # feat.Destroy()
dataSource.Destroy()

# 数据库检索
intersect_list = []
intersect_shp_list = []
query = '''SELECT ST_AsBinary(geom),* from %s 
WHERE cloud_cover<=%f AND sensing_time>='%s' AND sensing_time<='%s' '''%(tb,cloud,start_time,end_time)
pgisCursor.execute(query)
rows = pgisCursor.fetchall()
print "正在查询..."
for row in rows:
    geom_p = row[0]
    wkb = ogr.CreateGeometryFromWkb(bytes(geom_p))
    intersect_bool = ogr.Geometry.Intersect(geom_shp,wkb)
    if intersect_bool is True:
        intersect_shp_list.append(row)
        intersect_list.append(row[0])
        # data = [row[1] , row[3] , row[4] , row[5] , row[6] , str(row[7]) , str(row[9]) , row[12]]
        data = [row[4], row[12]]
        writer.writerow(data)

f.close()
print len(intersect_list),intersect_list

# 创建输出shp图层
# 为了支持中文路径，请添加下面这句代码
gdal.SetConfigOption("GDAL_FILENAME_IS_UTF8","NO")
# 为了使属性表字段支持中文，请添加下面这句
gdal.SetConfigOption("SHAPE_ENCODING","")
# 注册所有的驱动
ogr.RegisterAll()
# 数据格式的驱动
driver = ogr.GetDriverByName('ESRI Shapefile')
ds=driver.CreateDataSource(os.path.dirname(outshp))
shapLayer= ds.CreateLayer(os.path.basename(outshp).split('.')[0],geom_type=ogr.wkbPolygon)
# 添加字段
fieldDefnuid = ogr.FieldDefn('uid', ogr.OFTString)
fieldDefnuid.SetWidth(50)
shapLayer.CreateField(fieldDefnuid)
fieldDefnGR_ID = ogr.FieldDefn('GR_ID', ogr.OFTString)
fieldDefnGR_ID.SetWidth(100)
shapLayer.CreateField(fieldDefnGR_ID)
fieldDefnPRT_ID = ogr.FieldDefn('PRT_ID', ogr.OFTString)
fieldDefnPRT_ID.SetWidth(100)
shapLayer.CreateField(fieldDefnPRT_ID)
fieldDefnDATATAKE = ogr.FieldDefn('DATATAKE', ogr.OFTString)
fieldDefnDATATAKE.SetWidth(100)
shapLayer.CreateField(fieldDefnDATATAKE)
fieldDefnMGRS_TILE = ogr.FieldDefn('MGRS_TILE', ogr.OFTString)
fieldDefnMGRS_TILE.SetWidth(10)
shapLayer.CreateField(fieldDefnMGRS_TILE)
fieldDefnSENS_TIME = ogr.FieldDefn('SENS_TIME', ogr.OFTString)
fieldDefnSENS_TIME.SetWidth(50)
shapLayer.CreateField(fieldDefnSENS_TIME)
fieldDefnTOTAL_SIZE = ogr.FieldDefn('TOTAL_SIZE', ogr.OFTString)
fieldDefnTOTAL_SIZE.SetWidth(150)
shapLayer.CreateField(fieldDefnTOTAL_SIZE)
fieldDefnCLD_CVER = ogr.FieldDefn('CLD_CVER', ogr.OFTReal)
shapLayer.CreateField(fieldDefnCLD_CVER)
fieldDefnGEO_FLAG = ogr.FieldDefn('GEO_FLAG', ogr.OFTString)
fieldDefnGEO_FLAG.SetWidth(10)
shapLayer.CreateField(fieldDefnGEO_FLAG)
fieldDefnGEN_TIME = ogr.FieldDefn('GEN_TIME', ogr.OFTString)
fieldDefnGEN_TIME.SetWidth(50)
shapLayer.CreateField(fieldDefnGEN_TIME)
fieldDefnNORTH_LAT = ogr.FieldDefn('NORTH_LAT', ogr.OFTReal)
shapLayer.CreateField(fieldDefnNORTH_LAT)
fieldDefnSOUTH_LAT = ogr.FieldDefn('SOUTH_LAT', ogr.OFTReal)
shapLayer.CreateField(fieldDefnSOUTH_LAT)
fieldDefnWEST_LON = ogr.FieldDefn('WEST_LON', ogr.OFTReal)
shapLayer.CreateField(fieldDefnWEST_LON)
fieldDefnEAST_LON = ogr.FieldDefn('EAST_LON', ogr.OFTReal)
shapLayer.CreateField(fieldDefnEAST_LON)
fieldDefnBASE_URL = ogr.FieldDefn('BASE_URL', ogr.OFTString)
fieldDefnBASE_URL.SetWidth(150)
shapLayer.CreateField(fieldDefnBASE_URL)

for row_shp in intersect_shp_list:
    uid = row_shp[1]
    GRANULE_ID = row_shp[3]
    PRODUCT_ID = row_shp[4]
    # DATATAKE_IDENTIFIER = row_shp[5]
    # MGRS_TILE = row_shp[6]
    SENSING_TIME = str(row_shp[5])
    # TOTAL_SIZE = row_shp[8]
    CLOUD_COVER = float(row_shp[6])
    GEOMETRIC_QUALITY_FLAG = row_shp[7]
    # GENERATION_TIME = str(row_shp[11])
    NORTH_LAT = float(row_shp[8])
    SOUTH_LAT = float(row_shp[9])
    WEST_LON = float(row_shp[10])
    EAST_LON = float(row_shp[11])
    BASE_URL = row_shp[12]
    #创建feature
    defn = shapLayer.GetLayerDefn()
    feature = ogr.Feature(defn)
    #添加属性
    feature.SetField("uid", 0)
    feature.SetField("GR_ID",GRANULE_ID)
    feature.SetField("PRT_ID",PRODUCT_ID)
    # feature.SetField("DATATAKE", DATATAKE_IDENTIFIER)
    # feature.SetField("MGRS_TILE", MGRS_TILE)
    feature.SetField("SENS_TIME", SENSING_TIME)
    # feature.SetField("TOTAL_SIZE", TOTAL_SIZE)
    feature.SetField("CLD_CVER",CLOUD_COVER )
    feature.SetField("GEO_FLAG", GEOMETRIC_QUALITY_FLAG)
    # feature.SetField("GEN_TIME",GENERATION_TIME )
    # feature.SetField("NORTH_LAT", NORTH_LAT)
    # feature.SetField("SOUTH_LAT",SOUTH_LAT )
    # feature.SetField("WEST_LON", WEST_LON)
    # feature.SetField("EAST_LON",EAST_LON )
    feature.SetField("BASE_URL",BASE_URL )
    #添加坐标
    ring = ogr.Geometry(ogr.wkbLinearRing)
    ring.AddPoint(WEST_LON,NORTH_LAT)
    ring.AddPoint(WEST_LON, SOUTH_LAT)
    ring.AddPoint(EAST_LON, SOUTH_LAT)
    ring.AddPoint(EAST_LON, NORTH_LAT)
    ring.CloseRings()

    polygon = ogr.Geometry(ogr.wkbPolygon)
    polygon.AddGeometry(ring)

    feature.SetGeometry(polygon)
    shapLayer.CreateFeature(feature)
    feature.Destroy()
#指定投影
os.environ['GDAL_DATA'] = '/usr/share/gdal/2.2'
sr = osr.SpatialReference()
sr.ImportFromEPSG(4326)
prjFile = open(outshp.replace(".shp", ".prj"), 'w')
sr.MorphToESRI()
prjFile.write(sr.ExportToWkt())
prjFile.close()
ds.Destroy()

endTime = datetime.datetime.now()
print "ending time:", endTime
