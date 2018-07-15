# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'sataDataDownload_V0_test.ui'
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

class Ui_MainWindow(object):
    def setupUi(self, MainWindow):
        MainWindow.setObjectName(_fromUtf8("MainWindow"))
        MainWindow.resize(446, 440)
        self.centralwidget = QtGui.QWidget(MainWindow)
        self.centralwidget.setObjectName(_fromUtf8("centralwidget"))
        self.tabwidget = QtGui.QTabWidget(self.centralwidget)
        self.tabwidget.setGeometry(QtCore.QRect(10, 10, 421, 401))
        self.tabwidget.setObjectName(_fromUtf8("tabwidget"))
        self.tab = QtGui.QWidget()
        self.tab.setObjectName(_fromUtf8("tab"))
        self.outLabel_l = QtGui.QLabel(self.tab)
        self.outLabel_l.setGeometry(QtCore.QRect(320, 300, 71, 21))
        self.outLabel_l.setObjectName(_fromUtf8("outLabel_l"))
        self.searchBar_l = QtGui.QProgressBar(self.tab)
        self.searchBar_l.setGeometry(QtCore.QRect(110, 300, 201, 23))
        self.searchBar_l.setProperty("value", 24)
        self.searchBar_l.setObjectName(_fromUtf8("searchBar_l"))
        self.groupBox_4 = QtGui.QGroupBox(self.tab)
        self.groupBox_4.setGeometry(QtCore.QRect(10, 100, 391, 80))
        self.groupBox_4.setObjectName(_fromUtf8("groupBox_4"))
        self.sate_l = QtGui.QComboBox(self.groupBox_4)
        self.sate_l.setGeometry(QtCore.QRect(40, 30, 101, 22))
        self.sate_l.setObjectName(_fromUtf8("sate_l"))
        self.label_8 = QtGui.QLabel(self.groupBox_4)
        self.label_8.setGeometry(QtCore.QRect(10, 35, 54, 12))
        self.label_8.setObjectName(_fromUtf8("label_8"))
        self.label_9 = QtGui.QLabel(self.groupBox_4)
        self.label_9.setGeometry(QtCore.QRect(170, 34, 54, 12))
        self.label_9.setObjectName(_fromUtf8("label_9"))
        self.cloud_l = QtGui.QComboBox(self.groupBox_4)
        self.cloud_l.setGeometry(QtCore.QRect(220, 30, 101, 22))
        self.cloud_l.setObjectName(_fromUtf8("cloud_l"))
        self.label_10 = QtGui.QLabel(self.groupBox_4)
        self.label_10.setGeometry(QtCore.QRect(200, 30, 16, 21))
        self.label_10.setObjectName(_fromUtf8("label_10"))
        self.groupBox_5 = QtGui.QGroupBox(self.tab)
        self.groupBox_5.setGeometry(QtCore.QRect(10, 190, 391, 101))
        self.groupBox_5.setObjectName(_fromUtf8("groupBox_5"))
        self.label_11 = QtGui.QLabel(self.groupBox_5)
        self.label_11.setGeometry(QtCore.QRect(10, 30, 54, 12))
        self.label_11.setObjectName(_fromUtf8("label_11"))
        self.label_12 = QtGui.QLabel(self.groupBox_5)
        self.label_12.setGeometry(QtCore.QRect(10, 63, 54, 12))
        self.label_12.setObjectName(_fromUtf8("label_12"))
        self.savecsv_l = QtGui.QLineEdit(self.groupBox_5)
        self.savecsv_l.setGeometry(QtCore.QRect(60, 28, 241, 20))
        self.savecsv_l.setObjectName(_fromUtf8("savecsv_l"))
        self.savedownload_l = QtGui.QLineEdit(self.groupBox_5)
        self.savedownload_l.setGeometry(QtCore.QRect(60, 60, 241, 20))
        self.savedownload_l.setObjectName(_fromUtf8("savedownload_l"))
        self.slt_csv_bttn_l = QtGui.QPushButton(self.groupBox_5)
        self.slt_csv_bttn_l.setGeometry(QtCore.QRect(310, 27, 75, 23))
        self.slt_csv_bttn_l.setObjectName(_fromUtf8("slt_csv_bttn_l"))
        self.slt_downpath_bttn_l = QtGui.QPushButton(self.groupBox_5)
        self.slt_downpath_bttn_l.setGeometry(QtCore.QRect(310, 60, 75, 23))
        self.slt_downpath_bttn_l.setObjectName(_fromUtf8("slt_downpath_bttn_l"))
        self.downloadLabel_l = QtGui.QLabel(self.tab)
        self.downloadLabel_l.setGeometry(QtCore.QRect(320, 342, 54, 16))
        self.downloadLabel_l.setObjectName(_fromUtf8("downloadLabel_l"))
        self.downloadBar_l = QtGui.QProgressBar(self.tab)
        self.downloadBar_l.setGeometry(QtCore.QRect(110, 340, 201, 23))
        self.downloadBar_l.setProperty("value", 24)
        self.downloadBar_l.setObjectName(_fromUtf8("downloadBar_l"))
        self.search_bttn_l = QtGui.QPushButton(self.tab)
        self.search_bttn_l.setGeometry(QtCore.QRect(20, 300, 75, 23))
        self.search_bttn_l.setObjectName(_fromUtf8("search_bttn_l"))
        self.download_bttn_l = QtGui.QPushButton(self.tab)
        self.download_bttn_l.setGeometry(QtCore.QRect(20, 340, 75, 23))
        self.download_bttn_l.setObjectName(_fromUtf8("download_bttn_l"))
        self.groupBox_6 = QtGui.QGroupBox(self.tab)
        self.groupBox_6.setGeometry(QtCore.QRect(10, 10, 391, 81))
        self.groupBox_6.setObjectName(_fromUtf8("groupBox_6"))
        self.label_13 = QtGui.QLabel(self.groupBox_6)
        self.label_13.setGeometry(QtCore.QRect(16, 34, 54, 12))
        self.label_13.setObjectName(_fromUtf8("label_13"))
        self.label_14 = QtGui.QLabel(self.groupBox_6)
        self.label_14.setGeometry(QtCore.QRect(178, 34, 54, 12))
        self.label_14.setObjectName(_fromUtf8("label_14"))
        self.year_l = QtGui.QComboBox(self.groupBox_6)
        self.year_l.setGeometry(QtCore.QRect(40, 30, 101, 22))
        self.year_l.setObjectName(_fromUtf8("year_l"))
        self.month_l = QtGui.QComboBox(self.groupBox_6)
        self.month_l.setGeometry(QtCore.QRect(200, 30, 101, 22))
        self.month_l.setObjectName(_fromUtf8("month_l"))
        self.tabwidget.addTab(self.tab, _fromUtf8(""))
        self.tab_2 = QtGui.QWidget()
        self.tab_2.setObjectName(_fromUtf8("tab_2"))
        self.search_bttn_s = QtGui.QPushButton(self.tab_2)
        self.search_bttn_s.setGeometry(QtCore.QRect(20, 300, 75, 23))
        self.search_bttn_s.setObjectName(_fromUtf8("search_bttn_s"))
        self.download_bttn_s = QtGui.QPushButton(self.tab_2)
        self.download_bttn_s.setGeometry(QtCore.QRect(20, 340, 75, 23))
        self.download_bttn_s.setObjectName(_fromUtf8("download_bttn_s"))
        self.searchBar_s = QtGui.QProgressBar(self.tab_2)
        self.searchBar_s.setGeometry(QtCore.QRect(110, 300, 201, 23))
        self.searchBar_s.setProperty("value", 100)
        self.searchBar_s.setObjectName(_fromUtf8("searchBar_s"))
        self.downloadBar_s = QtGui.QProgressBar(self.tab_2)
        self.downloadBar_s.setGeometry(QtCore.QRect(110, 340, 201, 23))
        self.downloadBar_s.setProperty("value", 100)
        self.downloadBar_s.setObjectName(_fromUtf8("downloadBar_s"))
        self.outLabel_s = QtGui.QLabel(self.tab_2)
        self.outLabel_s.setGeometry(QtCore.QRect(320, 300, 71, 21))
        self.outLabel_s.setObjectName(_fromUtf8("outLabel_s"))
        self.downloadLabel_s = QtGui.QLabel(self.tab_2)
        self.downloadLabel_s.setGeometry(QtCore.QRect(320, 342, 54, 16))
        self.downloadLabel_s.setObjectName(_fromUtf8("downloadLabel_s"))
        self.groupBox_7 = QtGui.QGroupBox(self.tab_2)
        self.groupBox_7.setGeometry(QtCore.QRect(10, 10, 391, 81))
        self.groupBox_7.setObjectName(_fromUtf8("groupBox_7"))
        self.label_15 = QtGui.QLabel(self.groupBox_7)
        self.label_15.setGeometry(QtCore.QRect(16, 34, 54, 12))
        self.label_15.setObjectName(_fromUtf8("label_15"))
        self.label_16 = QtGui.QLabel(self.groupBox_7)
        self.label_16.setGeometry(QtCore.QRect(178, 34, 54, 12))
        self.label_16.setObjectName(_fromUtf8("label_16"))
        self.year_s = QtGui.QComboBox(self.groupBox_7)
        self.year_s.setGeometry(QtCore.QRect(40, 30, 101, 22))
        self.year_s.setObjectName(_fromUtf8("year_s"))
        self.month_s = QtGui.QComboBox(self.groupBox_7)
        self.month_s.setGeometry(QtCore.QRect(200, 30, 101, 22))
        self.month_s.setObjectName(_fromUtf8("month_s"))
        self.groupBox_8 = QtGui.QGroupBox(self.tab_2)
        self.groupBox_8.setGeometry(QtCore.QRect(10, 190, 391, 101))
        self.groupBox_8.setObjectName(_fromUtf8("groupBox_8"))
        self.label_17 = QtGui.QLabel(self.groupBox_8)
        self.label_17.setGeometry(QtCore.QRect(10, 30, 54, 12))
        self.label_17.setObjectName(_fromUtf8("label_17"))
        self.label_18 = QtGui.QLabel(self.groupBox_8)
        self.label_18.setGeometry(QtCore.QRect(10, 63, 54, 12))
        self.label_18.setObjectName(_fromUtf8("label_18"))
        self.savecsv_s = QtGui.QLineEdit(self.groupBox_8)
        self.savecsv_s.setGeometry(QtCore.QRect(60, 28, 241, 20))
        self.savecsv_s.setObjectName(_fromUtf8("savecsv_s"))
        self.savedownload_s = QtGui.QLineEdit(self.groupBox_8)
        self.savedownload_s.setGeometry(QtCore.QRect(60, 60, 241, 20))
        self.savedownload_s.setObjectName(_fromUtf8("savedownload_s"))
        self.slt_csv_bttn_s = QtGui.QPushButton(self.groupBox_8)
        self.slt_csv_bttn_s.setGeometry(QtCore.QRect(310, 27, 75, 23))
        self.slt_csv_bttn_s.setObjectName(_fromUtf8("slt_csv_bttn_s"))
        self.slt_downpath_bttn_s = QtGui.QPushButton(self.groupBox_8)
        self.slt_downpath_bttn_s.setGeometry(QtCore.QRect(310, 60, 75, 23))
        self.slt_downpath_bttn_s.setObjectName(_fromUtf8("slt_downpath_bttn_s"))
        self.groupBox_9 = QtGui.QGroupBox(self.tab_2)
        self.groupBox_9.setGeometry(QtCore.QRect(10, 100, 391, 80))
        self.groupBox_9.setObjectName(_fromUtf8("groupBox_9"))
        self.sate_s = QtGui.QComboBox(self.groupBox_9)
        self.sate_s.setGeometry(QtCore.QRect(40, 30, 101, 22))
        self.sate_s.setObjectName(_fromUtf8("sate_s"))
        self.label_19 = QtGui.QLabel(self.groupBox_9)
        self.label_19.setGeometry(QtCore.QRect(10, 35, 54, 12))
        self.label_19.setObjectName(_fromUtf8("label_19"))
        self.label_20 = QtGui.QLabel(self.groupBox_9)
        self.label_20.setGeometry(QtCore.QRect(170, 34, 54, 12))
        self.label_20.setObjectName(_fromUtf8("label_20"))
        self.cloud_s = QtGui.QComboBox(self.groupBox_9)
        self.cloud_s.setGeometry(QtCore.QRect(220, 30, 101, 22))
        self.cloud_s.setObjectName(_fromUtf8("cloud_s"))
        self.label_21 = QtGui.QLabel(self.groupBox_9)
        self.label_21.setGeometry(QtCore.QRect(200, 30, 16, 21))
        self.label_21.setObjectName(_fromUtf8("label_21"))
        self.tabwidget.addTab(self.tab_2, _fromUtf8(""))
        MainWindow.setCentralWidget(self.centralwidget)
        self.statusbar = QtGui.QStatusBar(MainWindow)
        self.statusbar.setObjectName(_fromUtf8("statusbar"))
        MainWindow.setStatusBar(self.statusbar)

        self.retranslateUi(MainWindow)
        self.tabwidget.setCurrentIndex(0)
        QtCore.QMetaObject.connectSlotsByName(MainWindow)

    def retranslateUi(self, MainWindow):
        MainWindow.setWindowTitle(_translate("MainWindow", "MainWindow", None))
        self.outLabel_l.setText(_translate("MainWindow", "TextLabel", None))
        self.groupBox_4.setTitle(_translate("MainWindow", "数据类型", None))
        self.label_8.setText(_translate("MainWindow", "卫星", None))
        self.label_9.setText(_translate("MainWindow", "云量", None))
        self.label_10.setText(_translate("MainWindow", "<=", None))
        self.groupBox_5.setTitle(_translate("MainWindow", "下载输出", None))
        self.label_11.setText(_translate("MainWindow", "csv保存", None))
        self.label_12.setText(_translate("MainWindow", "下载位置", None))
        self.slt_csv_bttn_l.setText(_translate("MainWindow", "选择", None))
        self.slt_downpath_bttn_l.setText(_translate("MainWindow", "选择", None))
        self.downloadLabel_l.setText(_translate("MainWindow", "TextLabel", None))
        self.search_bttn_l.setText(_translate("MainWindow", "开始检索", None))
        self.download_bttn_l.setText(_translate("MainWindow", "开始下载", None))
        self.groupBox_6.setTitle(_translate("MainWindow", "时间", None))
        self.label_13.setText(_translate("MainWindow", "年", None))
        self.label_14.setText(_translate("MainWindow", "月", None))
        self.tabwidget.setTabText(self.tabwidget.indexOf(self.tab), _translate("MainWindow", "Landsat", None))
        self.search_bttn_s.setText(_translate("MainWindow", "开始检索", None))
        self.download_bttn_s.setText(_translate("MainWindow", "开始下载", None))
        self.outLabel_s.setText(_translate("MainWindow", "TextLabel", None))
        self.downloadLabel_s.setText(_translate("MainWindow", "TextLabel", None))
        self.groupBox_7.setTitle(_translate("MainWindow", "时间", None))
        self.label_15.setText(_translate("MainWindow", "年", None))
        self.label_16.setText(_translate("MainWindow", "月", None))
        self.groupBox_8.setTitle(_translate("MainWindow", "下载输出", None))
        self.label_17.setText(_translate("MainWindow", "csv保存", None))
        self.label_18.setText(_translate("MainWindow", "下载位置", None))
        self.slt_csv_bttn_s.setText(_translate("MainWindow", "选择", None))
        self.slt_downpath_bttn_s.setText(_translate("MainWindow", "选择", None))
        self.groupBox_9.setTitle(_translate("MainWindow", "数据类型", None))
        self.label_19.setText(_translate("MainWindow", "卫星", None))
        self.label_20.setText(_translate("MainWindow", "云量", None))
        self.label_21.setText(_translate("MainWindow", "<=", None))
        self.tabwidget.setTabText(self.tabwidget.indexOf(self.tab_2), _translate("MainWindow", "Sentinel", None))

class Write_iamge_meta(QtGui.QMainWindow, Ui_MainWindow):
    def __init__(self):
        QtGui.QMainWindow.__init__(self)
        Ui_MainWindow.__init__(self)
        self.setupUi(self)
        # self.imagePathButton.clicked.connect(self.getImagePath)
        y = datetime.datetime.now().year
        i = 0
        while y >= 1970:
            self.year_l.insertItem(i,self.tr(str(y)))
            self.year_s.insertItem(i, self.tr(str(y)))
            i = i + 1
            y = y - 1

        m = 12
        j = 0
        while m >= 1:
            self.month_l.insertItem(j,self.tr(str(m)))
            self.month_s.insertItem(j, self.tr(str(m)))
            j = j + 1
            m = m - 1
        for c in range(0,105,5):
            self.cloud_l.insertItem(c,self.tr(str(c)))
            self.cloud_s.insertItem(c, self.tr(str(c)))

        satesList_l = ["landsat_1","landsat_2","landsat_3","landsat_4","landsat_5","landsat_7","landsat_8"]
        satesList_s = ["s1","s2a","s3"]
        for s in satesList_l:
            self.sate_l.insertItem(satesList_l.index(s),self.tr(s))
        for s in satesList_s:
            self.sate_s.insertItem(satesList_s.index(s),self.tr(s))

        self.sate_l.currentIndexChanged.connect(self.comboxchange)
        self.sate_s.currentIndexChanged.connect(self.comboxchange)
        self.slt_csv_bttn_l.clicked.connect(self.select_csv_l)
        self.slt_csv_bttn_s.clicked.connect(self.select_csv_s)
        self.slt_downpath_bttn_l.clicked.connect(self.downloadOutput_l)
        self.slt_downpath_bttn_s.clicked.connect(self.downloadOutput_s)
        self.search_bttn_l.clicked.connect(self.searchImages_l)
        self.search_bttn_s.clicked.connect(self.searchImages_s)
        self.download_bttn_l.clicked.connect(self.downloadImages_l)
        self.download_bttn_s.clicked.connect(self.downloadImages_s)
        self.searchBar_l.setMinimum(0)
        self.searchBar_l.setValue(0)
        self.searchBar_s.setMinimum(0)
        self.searchBar_s.setValue(0)
        self.downloadBar_l.setMinimum(0)
        self.downloadBar_l.setValue(0)
        self.downloadBar_s.setMinimum(0)
        self.downloadBar_s.setValue(0)
        self.outLabel_l.setText(u"等待检索")
        self.downloadLabel_l.setText(u"等待下载")
        self.outLabel_s.setText(u"等待检索")
        self.downloadLabel_s.setText(u"等待下载")

    def comboxchange(self):
        satetype = self.sate_l.currentText()
        satetype = self.sate_s.currentText()
        if satetype == "s1":
            self.cloud_s.clear()
        else:
            self.cloud_l.clear()
            self.cloud_s.clear()
            for c in range(0, 105,5):
                self.cloud_l.insertItem(c, self.tr(str(c)))
                self.cloud_s.insertItem(c, self.tr(str(c)))

    def select_csv_l(self):
        dir_path = QFileDialog.getSaveFileName(self,"save scv file",r"D:","csv (*.csv)")
        dir_path_utf = dir_path
        self.savecsv_l.setText(dir_path_utf)

    def select_csv_s(self):
        dir_path = QFileDialog.getSaveFileName(self,"save scv file",r"D:","csv (*.csv)")
        dir_path_utf = dir_path
        self.savecsv_s.setText(dir_path_utf)

    def downloadOutput_l(self):
        output_path = QFileDialog.getExistingDirectory(self, "Choose Download Folder", r"D:")
        output_path_utf = output_path
        self.savedownload_l.setText(output_path_utf)

    def downloadOutput_s(self):
        output_path = QFileDialog.getExistingDirectory(self, "Choose Download Folder", r"D:")
        output_path_utf = output_path
        self.savedownload_s.setText(output_path_utf)

    def searchImages_l(self):
        self.searchBar_l.setMinimum(0)
        self.searchBar_l.setValue(0)
        self.outLabel_l.setText(u"等待检索")
        # 输入待查询区域的shapefile文件
        shp_file = "./china/china.shp"
        # 检索参数设置
        cld = float(self.cloud_l.currentText())
        # start_time = '2017-05-01'  # 格式如 2017-01-01   2017-12-12
        # end_time = '2017-09-03'  # 格式如 2017-01-01   2017-12-12
        s_year = self.year_l.currentText()
        s_month = self.month_l.currentText()
        start_time = s_year + "-" + s_month + "-01"
        end_time =s_year + "-" + s_month + "-" + str(cal.monthrange(int(s_year), int(s_month))[1])

        f = open(self.savecsv_l.text(),'w')
        writer = csv.writer(f)
        # title = "uid" + "," + "granule_id" + "," + "product_id" + "," + "datatake_identifier" + "," + "mgrs_tile" + "," + "sensing_time" + "," + "cloud_cover" + "," + "base_url" +"\n"
        title = ["uid", "granule_id", "product_id", "date_acquired","cloud_cover","base_url"]
        print(title)
        writer.writerow(title)

        # 数据库配置参数
        db = "landsat"
        usr = "postgres"
        pw = "postgres"
        host = "10.16.20.27"
        port = "5432"
        tb = self.sate_l.currentText()  # landsat_1
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
        query = '''SELECT ST_AsBinary(geom),* from %s WHERE cloud_cover<=%f AND date_acquired>='%s' AND date_acquired<='%s' '''%(tb,cld,start_time,end_time)
#         query = '''SELECT ST_AsBinary(geom),* from %s WHERE cloud_cover<=%f AND date_acquired LIKE '%s' ''' % (tb, cld, start_time)
        #print query
        pgisCursor.execute(query)
        rows = pgisCursor.fetchall()
        readcount = len(rows)
        self.searchBar_l.setMaximum(readcount)
        print("Searching...")
        for k,row in enumerate(rows):
            geom_p = row[0]
            self.searchBar_l.setValue(k)
            self.outLabel_l.setText(str(k)+"/"+str(readcount))
            wkb = ogr.CreateGeometryFromWkb(bytes(geom_p))
            intersect_bool = ogr.Geometry.Intersect(geom_shp, wkb)
            if intersect_bool is True:
                intersect_shp_list.append(row)
                intersect_list.append(row[0])
                # title = ["uid", "granule_id", "product_id", "date_acquired","cloud_cover","base_url"]
                # print row[4]
                data = [row[1], row[3], row[4], row[5], str(row[6]), row[11]]
                writer.writerow(data)
        f.close()
        # print len(intersect_list), intersect_list
        endTime = datetime.datetime.now()
        self.outLabel_l.setText(u"检索完成")
        print("ending time:", endTime)

    def searchImages_s(self):
        self.searchBar_s.setMinimum(0)
        self.searchBar_s.setValue(0)
        self.outLabel_s.setText(u"等待检索")

        tb = self.sate_s.currentText()  # s1

        # 输入待查询区域的shapefile文件
        shp_file = "./china/china.shp"
        if tb == "s1":
            # 检索参数设置
            # cld = float(self.cloud_s.currentText())
            # start_time = '2017-05-01'  # 格式如 2017-01-01   2017-12-12
            # end_time = '2017-09-03'  # 格式如 2017-01-01   2017-12-12
            s_year = self.year_s.currentText()
            s_month = self.month_s.currentText()
            start_time = s_year + "-" + s_month + "-01"
            end_time = s_year + "-" + s_month + "-" + str(cal.monthrange(int(s_year), int(s_month))[1])

            f = open(self.savecsv_s.text(), 'wb')
            writer = csv.writer(f)
            # title = "uid" + "," + "granule_id" + "," + "product_id" + "," + "datatake_identifier" + "," + "mgrs_tile" + "," + "sensing_time" + "," + "cloud_cover" + "," + "base_url" +"\n"
            title = ["filename", "uuid"]
            print(title)
            writer.writerow(title)

            # 数据库配置参数
            db = "sentinel"
            usr = "postgres"
            pw = "postgres"
            host = "10.16.20.27"
            port = "5432"
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
            query = '''SELECT ST_AsBinary(geom),* from %s WHERE beginposition>='%s' AND beginposition<'%s' ''' % (
            tb, start_time, end_time)
            #         query = '''SELECT ST_AsBinary(geom),* from %s WHERE cloud_cover<=%f AND date_acquired LIKE '%s' ''' % (tb, cld, start_time)
            #print query
            pgisCursor.execute(query)
            rows = pgisCursor.fetchall()
            readcount = len(rows)
            self.searchBar_s.setMaximum(readcount)
            print("Searching...")
            for k, row in enumerate(rows):
                geom_p = row[0]
                self.searchBar_s.setValue(k)
                self.outLabel_s.setText(str(k) + "/" + str(readcount))
                wkb = ogr.CreateGeometryFromWkb(bytes(geom_p))
                intersect_bool = ogr.Geometry.Intersect(geom_shp, wkb)
                if intersect_bool is True:
                    intersect_shp_list.append(row)
                    intersect_list.append(row[0])
                    # title = ["uid", "granule_id", "product_id", "date_acquired","cloud_cover","base_url"]
                    # print row[4]
                    data = [row[2], row[15]]
                    writer.writerow(data)
            f.close()
            # print len(intersect_list), intersect_list
            endTime = datetime.datetime.now()
            self.outLabel_s.setText(u"检索完成")
            print("ending time:", endTime)
        elif tb == "s2a":
            # 检索参数设置
            cld = float(self.cloud_s.currentText())
            # start_time = '2017-05-01'  # 格式如 2017-01-01   2017-12-12
            # end_time = '2017-09-03'  # 格式如 2017-01-01   2017-12-12
            s_year = self.year_s.currentText()
            s_month = self.month_s.currentText()
            start_time = s_year + "-" + s_month + "-01"
            end_time = s_year + "-" + s_month + "-" + str(cal.monthrange(int(s_year), int(s_month))[1])

            with open(self.savecsv_s.text(), 'w') as f:
                writer = csv.writer(f)
                # title = "uid" + "," + "granule_id" + "," + "product_id" + "," + "datatake_identifier" + "," + "mgrs_tile" + "," + "sensing_time" + "," + "cloud_cover" + "," + "base_url" +"\n"
                title = ['product_id', 'base_url']
                print(title)
                writer.writerow(title)

                # 数据库配置参数
                db = "sentinel"
                usr = "postgres"
                pw = "postgres"
                host = "10.16.20.27"
                port = "5432"
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

                query = '''SELECT ST_AsBinary(geom),* from %s WHERE cloud_cover<=%f AND sensing_time>='%s' AND sensing_time<='%s' ''' % (
                tb, cld, start_time, end_time)
                #         query = '''SELECT ST_AsBinary(geom),* from %s WHERE cloud_cover<=%f AND date_acquired LIKE '%s' ''' % (tb, cld, start_time)
                #print query
                pgisCursor.execute(query)
                rows = pgisCursor.fetchall()
                readcount = len(rows)
                self.searchBar_s.setMaximum(readcount)
                print("Searching...")
                for k, row in enumerate(rows):
                    geom_p = row[0]
                    self.searchBar_s.setValue(k)
                    self.outLabel_s.setText(str(k) + "/" + str(readcount))
                    wkb = ogr.CreateGeometryFromWkb(bytes(geom_p))
                    intersect_bool = ogr.Geometry.Intersect(geom_shp, wkb)
                    if intersect_bool is True:
                        intersect_shp_list.append(row)
                        intersect_list.append(row[0])
                        # title = ["uid", "granule_id", "product_id", "date_acquired","cloud_cover","base_url"]
                        # print row[4]
                        data = [row[4], row[12]]
                        writer.writerow(data)
		    #f.close()
		    # print len(intersect_list), intersect_list
            endTime = datetime.datetime.now()
            self.outLabel_s.setText(u"检索完成")
            print("ending time:", endTime)

    def geturl_l(self,ourl):
        suffix_list = ['ANG.txt', 'B1.TIF', 'B2.TIF', 'B3.TIF', 'B4.TIF', 'B5.TIF', 'B6.TIF', 'B7.TIF', 'B8.TIF',
                       'B9.TIF',
                       'B10.TIF', 'B11.TIF', 'BQA.TIF', 'MTL.txt']
        url_pre = ourl.split('/')[-1]
        url = []
        for suffix in suffix_list:
            url.append(ourl + '/' + url_pre + '_' + suffix)
        return url

    def downloadImages_l(self):
        self.downloadBar_l.setMinimum(0)
        self.downloadBar_l.setValue(0)
        self.downloadLabel_l.setText(u"等待下载")
        idfile = self.savecsv_l.text()
        outputdir = self.savedownload_l.text()
        csvf = open(idfile,'r')
        csv_count = len(csvf.readlines()) -1
        csvf.close()
        print(csv_count)
        self.downloadBar_l.setMaximum(csv_count)
        with open(idfile, 'r') as f:
            idcsv = csv.reader(f)
            header = next(idcsv)
            for ck,row in enumerate(idcsv):
                self.downloadBar_l.setValue(ck)
                sid = row[2]
                ourl = row[5]
                urlpaths = self.geturl_l(ourl)
                alldir = os.path.join(outputdir, sid)
                if os.path.exists(outputdir + '/{}'.format(sid)):
                    shutil.rmtree(outputdir + '/{}'.format(sid))
                if not os.path.exists(outputdir + '/{}.zip'.format(sid)):
                    ct = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                    self.downloadLabel_l.setText(str(ck) + "/" + str(csv_count))
                    print(ct + 'Downloading {}...'.format(sid))
                    for urlpath in urlpaths:
                        if not os.path.exists(alldir):
                            os.makedirs(alldir)
                        ttt = download('{}'.format(urlpath), alldir)
                        # self.downloadLabel.setText(ttt["speed"])
                    dt = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                    print(dt + 'compressing......')
                    shutil.make_archive(outputdir + '/{}'.format(sid), 'zip', alldir)
                    shutil.rmtree(alldir)

    def get_image_s(self,gra_id, url, outputdir):
        """
        fetch xml and image from google could"""
        target_path = os.path.join(outputdir, gra_id)
        target_manifest = os.path.join(target_path, "manifest.safe")
        if not os.path.exists(target_path):
            os.makedirs(target_path)
            manifest_url = url + "/manifest.safe"
            content = urlopen(manifest_url)
            with open(target_manifest, 'wb') as f:
                shutil.copyfileobj(content, f)
        safexml = etree.parse(target_manifest)
        filemetas = safexml.xpath('//byteStream/fileLocation/@href')
        dMGRS = url.split('/')[-1].split('_')[-2]
        for filepath in filemetas:
            abs_path = os.path.join(target_path, gra_id, *filepath.split('/')[3:-1])
            url_path = os.path.join(url, *filepath.split('/')[1:])
            try:
                sMGRS = filepath.split('/')[-1].split('_')[:]
            except:
                pass
            dirname = filepath.split('/')[1]
            if dMGRS in sMGRS and dirname == 'GRANULE' or dMGRS + '.xml' in sMGRS and \
                            dirname == 'GRANULE' or 'MTD_TL.xml' in url_path:
                if not os.path.exists(abs_path):
                    os.makedirs(abs_path)
                download(url_path, abs_path)
        ct = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
        print(ct + 'compressing......')
        shutil.make_archive(outputdir + '/{}'.format(gra_id), 'zip', target_path)
        shutil.rmtree(target_path)

    def downloadImages_s(self):
        S1_download_url = 'https://scihub.copernicus.eu/dhus'
        api = SentinelAPI('chenjinlv', 'cjl19890710', S1_download_url)
        self.downloadBar_s.setMinimum(0)
        self.downloadBar_s.setValue(0)
        self.downloadLabel_s.setText(u"等待下载")
        idfile = self.savecsv_s.text()
        outputdir = self.savedownload_s.text()
        if self.sate_s.currentText() == "s2a":
            csvf = open(idfile,'r')
            csv_count = len(csvf.readlines()) -1
            csvf.close()
            print(csv_count)
            self.downloadBar_s.setMaximum(csv_count)
            with open(idfile, 'r') as f:
                idcsv = csv.reader(f)
                header = next(idcsv)
                for ck,row in enumerate(idcsv):
                    self.downloadBar_s.setValue(ck)
                    url = row[1]
                    PRT_ID = row[0]
                    # filename = url.split('/')[-1]
                    if os.path.exists(outputdir + '/{}'.format(PRT_ID)):
                        shutil.rmtree(outputdir + '/{}'.format(PRT_ID))
                    if not os.path.exists(outputdir + '/{}.zip'.format(PRT_ID)):
                        dt = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                        self.downloadLabel_s.setText(str(ck) + "/" + str(csv_count))
                        print(dt + "Downloading {} ...".format(PRT_ID))
                        self.get_image_s(PRT_ID, url, outputdir)
        elif self.sate_s.currentText() == "s1":
            csvf = open(idfile,'r')
            csv_count = len(csvf.readlines()) -1
            csvf.close()
            print(csv_count)
            self.downloadBar_s.setMaximum(csv_count)
            with open(idfile, 'r') as f:
                idcsv = csv.reader(f)
                header = next(idcsv)
                for ck,row in enumerate(idcsv):
                    self.downloadBar_s.setValue(ck)
                    uuidprd = row[1]
                    filename = row[0]
                    self.downloadLabel_s.setText(str(ck) + "/" + str(csv_count))
                    api.download(uuidprd, directory_path=outputdir)


if __name__ == "__main__":
    imageapp = QtGui.QApplication(sys.argv)
    window = Write_iamge_meta()
    window.show()
    sys.exit(imageapp.exec_())
