# -*- conding: utf-8 -*-

import os
import time
import shutil
from lxml import etree
from homura import download
import csv
try:
    from urllib2 import urlopen
    from urllib2 import HTTPError
except ImportError:
    from urllib.request import urlopen, HTTPError


s2acsv = './sentinel2_cn_201709.csv'
outputdir = '/media/tq/sz-co-09653/Sentinel2_1709'


def get_image(gra_id, url, path):
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


with open('{}'.format(s2acsv), 'r') as ofile:
    s2a_csv = csv.reader(ofile)
    header = next(s2a_csv)
    for row in s2a_csv:
        url = row[10]
        PRT_ID = row[1]
        # filename = url.split('/')[-1]
        if os.path.exists(outputdir + '/{}'.format(PRT_ID)):
            shutil.rmtree(outputdir + '/{}'.format(PRT_ID))
        if not os.path.exists(outputdir + '/{}.zip'.format(PRT_ID)):
            dt = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
            print(dt + "Downloading {} ...".format(PRT_ID))
            get_image(PRT_ID, url, outputdir)
