#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import time
import shutil
import csv
from homura import download

idfile = '/home/tq/Music/landsat5.csv'
outputdir = '/home/tq/Music'



def geturl(sid, ourl):
    suffix_list = ['ANG.txt', 'B1.TIF', 'B2.TIF', 'B3.TIF', 'B4.TIF', 'B5.TIF', 'B6.TIF', 'B7.TIF', 'B8.TIF', 'B9.TIF',
                   'B10.TIF', 'B11.TIF', 'BQA.TIF', 'MTL.txt']
    url_pre = ourl.split('/')[-1]
    url = []
    for suffix in suffix_list:
        url.append(ourl + '/' + url_pre + '_' + suffix)
    return url


def imlf(idfile):
    with open(idfile, 'r') as f:
        idcsv = csv.reader(f)
        header = next(idcsv)
        for row in idcsv:
            sid = row[1]
            ourl = row[9]
            urlpaths = geturl(sid, ourl)
            alldir = os.path.join(outputdir, sid)
            if os.path.exists(outputdir + '/{}'.format(sid)):
                shutil.rmtree(outputdir + '/{}'.format(sid))
            if not os.path.exists(outputdir + '/{}.zip'.format(sid)):
                ct = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
                print(ct + 'Downloading {}...'.format(sid))
                for urlpath in urlpaths:
                    if not os.path.exists(alldir):
                        os.makedirs(alldir)
                    download('{}'.format(urlpath), alldir)
                dt = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
                print(dt + 'compressing......')
                shutil.make_archive(outputdir + '/{}'.format(sid), 'zip', alldir)
                shutil.rmtree(alldir)


if __name__ == '__main__':
    imlf(idfile)
