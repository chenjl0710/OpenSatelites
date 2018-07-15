# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'SAR_DOWNLOADER_UI.ui'
#
# Created by: PyQt4 UI code generator 4.11.4
#
# WARNING! All changes made in this file will be lost!
import sys
from PyQt4 import QtCore, QtGui
from PyQt4.QtGui import QFileDialog
from lxml import etree
import shutil
import csv,os,time
import psycopg2
import datetime
import calendar as cal
from homura import download
from sentinelsat.sentinel import SentinelAPI
try:
    from urllib2 import urlopen
    from urllib2 import HTTPError
except ImportError:
    from urllib.request import urlopen, HTTPError
try:
    from osgeo import gdal, ogr, osr
except ImportError:
    import gdal, ogr, osr

try:
    _fromUtf8 = QtCore.QString.fromUtf8
except AttributeError:
    def _fromUtf8(s):
        return s

try:
    _encoding = QtGui.QApplication.UnicodeUTF8
    def _translate(context, text, disambig):
        return QtGui.QApplication.translate(context, text, disambig, _encoding)
except AttributeError:
    def _translate(context, text, disambig):
        return QtGui.QApplication.translate(context, text, disambig)

class Ui_mainWindow(object):
    def setupUi(self, mainWindow):
        mainWindow.setObjectName(_fromUtf8("mainWindow"))
        mainWindow.resize(477, 433)
        self.centralwidget = QtGui.QWidget(mainWindow)
        self.centralwidget.setObjectName(_fromUtf8("centralwidget"))
        self.search_bttn_sar_4 = QtGui.QPushButton(self.centralwidget)
        self.search_bttn_sar_4.setGeometry(QtCore.QRect(30, 290, 75, 23))
        self.search_bttn_sar_4.setObjectName(_fromUtf8("search_bttn_sar_4"))
        self.download_bttn_sar_4 = QtGui.QPushButton(self.centralwidget)
        self.download_bttn_sar_4.setGeometry(QtCore.QRect(30, 330, 75, 23))
        self.download_bttn_sar_4.setObjectName(_fromUtf8("download_bttn_sar_4"))
        self.searchBar_sar_4 = QtGui.QProgressBar(self.centralwidget)
        self.searchBar_sar_4.setGeometry(QtCore.QRect(120, 290, 201, 23))
        self.searchBar_sar_4.setProperty("value", 24)
        self.searchBar_sar_4.setObjectName(_fromUtf8("searchBar_sar_4"))
        self.groupBox_19 = QtGui.QGroupBox(self.centralwidget)
        self.groupBox_19.setGeometry(QtCore.QRect(20, 180, 391, 101))
        self.groupBox_19.setObjectName(_fromUtf8("groupBox_19"))
        self.label_42 = QtGui.QLabel(self.groupBox_19)
        self.label_42.setGeometry(QtCore.QRect(10, 30, 54, 12))
        self.label_42.setObjectName(_fromUtf8("label_42"))
        self.label_43 = QtGui.QLabel(self.groupBox_19)
        self.label_43.setGeometry(QtCore.QRect(10, 63, 54, 12))
        self.label_43.setObjectName(_fromUtf8("label_43"))
        self.savecsv_sar_4 = QtGui.QLineEdit(self.groupBox_19)
        self.savecsv_sar_4.setGeometry(QtCore.QRect(60, 28, 241, 20))
        self.savecsv_sar_4.setObjectName(_fromUtf8("savecsv_sar_4"))
        self.savedownload_sar_4 = QtGui.QLineEdit(self.groupBox_19)
        self.savedownload_sar_4.setGeometry(QtCore.QRect(60, 60, 241, 20))
        self.savedownload_sar_4.setObjectName(_fromUtf8("savedownload_sar_4"))
        self.slt_csv_bttn_sar_4 = QtGui.QPushButton(self.groupBox_19)
        self.slt_csv_bttn_sar_4.setGeometry(QtCore.QRect(310, 27, 75, 23))
        self.slt_csv_bttn_sar_4.setObjectName(_fromUtf8("slt_csv_bttn_sar_4"))
        self.slt_downpath_bttn_sar_4 = QtGui.QPushButton(self.groupBox_19)
        self.slt_downpath_bttn_sar_4.setGeometry(QtCore.QRect(310, 60, 75, 23))
        self.slt_downpath_bttn_sar_4.setObjectName(_fromUtf8("slt_downpath_bttn_sar_4"))
        self.downloadBar_sar_4 = QtGui.QProgressBar(self.centralwidget)
        self.downloadBar_sar_4.setGeometry(QtCore.QRect(120, 330, 201, 23))
        self.downloadBar_sar_4.setProperty("value", 24)
        self.downloadBar_sar_4.setObjectName(_fromUtf8("downloadBar_sar_4"))
        self.outLabel_sar_4 = QtGui.QLabel(self.centralwidget)
        self.outLabel_sar_4.setGeometry(QtCore.QRect(330, 290, 71, 21))
        self.outLabel_sar_4.setObjectName(_fromUtf8("outLabel_sar_4"))
        self.downloadLabel_sar3_2 = QtGui.QLabel(self.centralwidget)
        self.downloadLabel_sar3_2.setGeometry(QtCore.QRect(330, 332, 71, 16))
        self.downloadLabel_sar3_2.setObjectName(_fromUtf8("downloadLabel_sar3_2"))
        self.groupBox_20 = QtGui.QGroupBox(self.centralwidget)
        self.groupBox_20.setGeometry(QtCore.QRect(20, 12, 391, 80))
        self.groupBox_20.setObjectName(_fromUtf8("groupBox_20"))
        self.sate_sar_4 = QtGui.QComboBox(self.groupBox_20)
        self.sate_sar_4.setGeometry(QtCore.QRect(70, 30, 101, 22))
        self.sate_sar_4.setObjectName(_fromUtf8("sate_sar_4"))
        self.label_37 = QtGui.QLabel(self.groupBox_20)
        self.label_37.setGeometry(QtCore.QRect(10, 35, 54, 12))
        self.label_37.setObjectName(_fromUtf8("label_37"))
        self.groupBox_21 = QtGui.QGroupBox(self.centralwidget)
        self.groupBox_21.setGeometry(QtCore.QRect(20, 92, 391, 81))
        self.groupBox_21.setObjectName(_fromUtf8("groupBox_21"))
        self.label_44 = QtGui.QLabel(self.groupBox_21)
        self.label_44.setGeometry(QtCore.QRect(16, 34, 54, 12))
        self.label_44.setObjectName(_fromUtf8("label_44"))
        self.year_sar_4 = QtGui.QComboBox(self.groupBox_21)
        self.year_sar_4.setGeometry(QtCore.QRect(70, 30, 101, 22))
        self.year_sar_4.setObjectName(_fromUtf8("year_sar_4"))
        mainWindow.setCentralWidget(self.centralwidget)
        self.menubar = QtGui.QMenuBar(mainWindow)
        self.menubar.setGeometry(QtCore.QRect(0, 0, 477, 25))
        self.menubar.setObjectName(_fromUtf8("menubar"))
        mainWindow.setMenuBar(self.menubar)
        self.statusbar = QtGui.QStatusBar(mainWindow)
        self.statusbar.setObjectName(_fromUtf8("statusbar"))
        mainWindow.setStatusBar(self.statusbar)

        self.retranslateUi(mainWindow)
        QtCore.QMetaObject.connectSlotsByName(mainWindow)

    def retranslateUi(self, mainWindow):
        mainWindow.setWindowTitle(_translate("mainWindow", "SAR DATA DOWNLOADER", None))
        self.search_bttn_sar_4.setText(_translate("mainWindow", "开始检索", None))
        self.download_bttn_sar_4.setText(_translate("mainWindow", "开始下载", None))
        self.groupBox_19.setTitle(_translate("mainWindow", "下载输出", None))
        self.label_42.setText(_translate("mainWindow", "csv保存", None))
        self.label_43.setText(_translate("mainWindow", "下载位置", None))
        self.slt_csv_bttn_sar_4.setText(_translate("mainWindow", "选择", None))
        self.slt_downpath_bttn_sar_4.setText(_translate("mainWindow", "选择", None))
        self.outLabel_sar_4.setText(_translate("mainWindow", "TextLabel", None))
        self.downloadLabel_sar3_2.setText(_translate("mainWindow", "TextLabel", None))
        self.groupBox_20.setTitle(_translate("mainWindow", "数据类型", None))
        self.label_37.setText(_translate("mainWindow", "卫星", None))
        self.groupBox_21.setTitle(_translate("mainWindow", "时间", None))
        self.label_44.setText(_translate("mainWindow", "年-月", None))

class sarData_downloader(QtGui.QMainWindow, Ui_mainWindow):
    def __init__(self):
        QtGui.QMainWindow.__init__(self)
        Ui_mainWindow.__init__(self)
        self.setupUi(self)
        # self.imagePathButton.clicked.connect(self.getImagePath)
        # y = datetime.datetime.now().year
        # i = 0
        # while y >= 1970:
        #     self.year_l.insertItem(i,self.tr(str(y)))
        #     self.year_s.insertItem(i, self.tr(str(y)))
        #     i = i + 1
        #     y = y - 1
        #
        # m = 12
        # j = 0
        # while m >= 1:
        #     self.month_l.insertItem(j,self.tr(str(m)))
        #     self.month_s.insertItem(j, self.tr(str(m)))
        #     j = j + 1
        #     m = m - 1
        # for c in range(0,105,5):
        #     self.cloud_l.insertItem(c,self.tr(str(c)))
        #     self.cloud_s.insertItem(c, self.tr(str(c)))

        sar_list = ["airsar","alos","ers_1","ers_2","jers_1","radarsat_1","sentinel_1a","sentinel_1b","smap","uavsar"]
        for s in sar_list:
            self.sate_sar_4.insertItem(sar_list.index(s),self.tr(s))

        self.sate_sar_4.currentIndexChanged.connect(self.comboxchange)
        # self.sate_s.currentIndexChanged.connect(self.comboxchange)
        self.slt_csv_bttn_sar_4.clicked.connect(self.select_csv_l_sar)
        # self.slt_csv_bttn_s.clicked.connect(self.select_csv_s)
        self.slt_downpath_bttn_sar_4.clicked.connect(self.downloadOutput_l_sar)
        # self.slt_downpath_bttn_s.clicked.connect(self.downloadOutput_s)
        self.search_bttn_sar_4.clicked.connect(self.searchImages_l_sar)
        # self.search_bttn_s.clicked.connect(self.searchImages_s)
        self.download_bttn_sar_4.clicked.connect(self.downloadImages_l_sar)
        # self.download_bttn_s.clicked.connect(self.downloadImages_s)
        self.searchBar_sar_4.setMinimum(0)
        self.searchBar_sar_4.setValue(0)
        # self.searchBar_s.setMinimum(0)
        # self.searchBar_s.setValue(0)
        self.downloadBar_sar_4.setMinimum(0)
        self.downloadBar_sar_4.setValue(0)
        # self.downloadBar_s.setMinimum(0)
        # self.downloadBar_s.setValue(0)
        self.outLabel_sar_4.setText(u"等待检索")
        self.downloadLabel_sar3_2.setText(u"等待下载")
        # self.outLabel_s.setText(u"等待检索")
        # self.downloadLabel_s.setText(u"等待下载")


    def downloadImages_l_sar(self):
        self.downloadBar_sar_4.setMinimum(0)
        self.downloadBar_sar_4.setValue(0)
        self.downloadLabel_sar3_2.setText(u"等待下载")
        idfile = self.savecsv_sar_4.text()
        outputdir = self.savedownload_sar_4.text()
        csvf = open(idfile,'r')
        csv_count = len(csvf.readlines()) -1
        csvf.close()
        print(csv_count)
        self.downloadBar_sar_4.setMaximum(csv_count)
        with open(idfile, 'r') as f:
            idcsv = csv.reader(f)
            header = next(idcsv)
            for ck,row in enumerate(idcsv):
                self.downloadBar_sar_4.setValue(ck)
                sid = row[0]
                ourl = row[20]
                outputFile = os.path.join(outputdir, sid+".zip")
                httpRequest = '''wget --check-certificate=off -c --http-user=chenjinlv --http-password="Cjl19890710!" -O {0} "{1}"'''.format(
                    outputFile, ourl)
                print(httpRequest)
                os.system(httpRequest)
                # urlpaths = self.geturl_l(ourl)
                # alldir = os.path.join(outputdir, sid)
                # if os.path.exists(outputdir + '/{}'.format(sid)):
                #     shutil.rmtree(outputdir + '/{}'.format(sid))
                # if not os.path.exists(outputdir + '/{}.zip'.format(sid)):
                #     ct = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                #     self.downloadLabel_l.setText(str(ck) + "/" + str(csv_count))
                #     print(ct + 'Downloading {}...'.format(sid))
                #     for urlpath in urlpaths:
                #         if not os.path.exists(alldir):
                #             os.makedirs(alldir)
                #         ttt = download('{}'.format(urlpath), alldir)
                #         # self.downloadLabel.setText(ttt["speed"])
                #     dt = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                #     print(dt + 'compressing......')
                #     shutil.make_archive(outputdir + '/{}'.format(sid), 'zip', alldir)
                #     shutil.rmtree(alldir)

    def searchImages_l_sar(self):
        self.searchBar_sar_4.setMinimum(0)
        self.searchBar_sar_4.setValue(0)
        self.outLabel_sar_4.setText(u"等待检索")
        # 输入待查询区域的shapefile文件
        shp_file = "./china/china.shp"
        # 检索参数设置
        # cld = float(self.cloud_l.currentText())
        # start_time = '2017-05-01'  # 格式如 2017-01-01   2017-12-12
        # end_time = '2017-09-03'  # 格式如 2017-01-01   2017-12-12
        sar_year = self.year_sar_4.currentText()
        # s_month = self.month_l.currentText()
        start_time = sar_year + '-01'
        end_time = sar_year + "-" + str(cal.monthrange(int(sar_year.split('-')[0]), int(sar_year.split('-')[1]))[1])

        f = open(self.savecsv_sar_4.text(), 'w')
        writer = csv.writer(f)
        # title = "uid" + "," + "granule_id" + "," + "product_id" + "," + "datatake_identifier" + "," + "mgrs_tile" + "," + "sensing_time" + "," + "cloud_cover" + "," + "base_url" +"\n"
        title = ["granule_name", "platform", "sensor", "beam_mode", "beam_mode_description", "orbit","path_number","frame_number","acquisition_date","processing_level",
                 "center_lat","center_lon","near_start_lat","near_start_lon","far_start_lat","far_start_lon","near_end_lat","near_end_lon","far_end_lat","far_end_lon","url"]
        print(title)
        writer.writerow(title)

        # 数据库配置参数
        db = "SAR"
        usr = "postgres"
        pw = "postgres"
        host = "10.16.20.27"
        port = "5432"
        tb = self.sate_sar_4.currentText()  # landsat_1
        # 连接数据库
        pgisCon = psycopg2.connect(database=db, user=usr, password=pw, host=host, port=port)
        pgisCursor = pgisCon.cursor()

        # 获取输入矢量的geometry
        driver = ogr.GetDriverByName('ESRI Shapefile')
        dataSource = driver.Open(shp_file, 0)
        if dataSource is not None:
            layer = dataSource.GetLayer(0)
            feat = layer.GetNextFeature()
            # while feat:
            geom_shp = feat.GetGeometryRef()
        dataSource.Destroy()

        # 数据库检索
        intersect_list = []
        intersect_shp_list = []
        query = '''SELECT ST_AsBinary(geom),* from %s WHERE acquisition_date>='%s' AND acquisition_date<='%s' ''' % (
        tb, start_time, end_time)
        #         query = '''SELECT ST_AsBinary(geom),* from %s WHERE cloud_cover<=%f AND date_acquired LIKE '%s' ''' % (tb, cld, start_time)
        # print query
        pgisCursor.execute(query)
        rows = pgisCursor.fetchall()
        readcount = len(rows)
        self.searchBar_sar_4.setMaximum(readcount)
        print("Searching...")
        for k, row in enumerate(rows):
            geom_p = row[0]
            self.searchBar_sar_4.setValue(k)
            self.outLabel_sar_4.setText(str(k) + "/" + str(readcount))
            wkb = ogr.CreateGeometryFromWkb(bytes(geom_p))
            intersect_bool = ogr.Geometry.Intersect(geom_shp, wkb)
            if intersect_bool is True:
                intersect_shp_list.append(row)
                intersect_list.append(row[0])
                # title = ["uid", "granule_id", "product_id", "date_acquired","cloud_cover","base_url"]
                # print row[4]
                data = [row[1],row[2], row[3], row[4], row[5], row[6],row[7],row[8],row[9],row[10],row[11],row[12],row[13],row[14],row[15],row[16],row[17],row[18],row[19],row[20], row[22]]
                writer.writerow(data)
        f.close()
        # print len(intersect_list), intersect_list
        endTime = datetime.datetime.now()
        self.outLabel_sar_4.setText(u"检索完成")
        print("ending time:", endTime)


    def downloadOutput_l_sar(self):
        output_path = QFileDialog.getExistingDirectory(self, "Choose Download Folder", r"D:")
        output_path_utf = output_path
        self.savedownload_sar_4.setText(output_path_utf)

    def select_csv_l_sar(self):
        dir_path = QFileDialog.getSaveFileName(self,"save scv file",r"D:","csv (*.csv)")
        dir_path_utf = dir_path
        self.savecsv_sar_4.setText(dir_path_utf)

    def comboxchange(self):
        # 数据库配置参数
        db = "SAR"
        usr = "postgres"
        pw = "postgres"
        host = "10.16.20.27"
        port = "5432"
        tb = self.sate_sar_4.currentText()  # landsat_1
        # 连接数据库
        pgisCon = psycopg2.connect(database=db, user=usr, password=pw, host=host, port=port)
        pgisCursor = pgisCon.cursor()
        search_str = '''select to_char(acquisition_date, 'YYYY-MM') as d from {0} group by d'''.format(tb)
        pgisCursor.execute(search_str)
        rows = pgisCursor.fetchall()
        ym = []
        for i in rows:
            ym.append(i[0])
        ym.sort()
        for yymm in ym:
            self.year_sar_4.insertItem(ym.index(yymm),self.tr(yymm))


if __name__ == "__main__":
    imageapp = QtGui.QApplication(sys.argv)
    window = sarData_downloader()
    window.show()
    sys.exit(imageapp.exec_())