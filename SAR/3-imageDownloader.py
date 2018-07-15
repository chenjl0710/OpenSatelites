# -*- coding: utf-8 -*-
import os

outputFolder = "./meta"
url = "https://datapool.asf.alaska.edu/RAW/SA/S1A_IW_RAW__0SDV_20171224T095458_20171224T095530_019843_021C19_6B74.zip"
outputFile = os.path.join(outputFolder,url.split('/')[-1])
httpRequest = '''wget --check-certificate=off -c --http-user=chenjinlv --http-password="Cjl19890710!" -O {0} "{1}"'''.format(outputFile,url)
print(httpRequest)
os.system(httpRequest)