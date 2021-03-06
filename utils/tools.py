__author__ = 'mengpeng'
import os
import csv
import time


def gethash(string, cap=0xffffffff):
    return hash(string) & cap


def date2num(date):
    """Parse date format 01/17/14 into int 20140117"""
    parts = date.split('/')
    if len(parts) != 3:
        parts = time.strftime("%m/%d/%y", time.localtime()).split('/')
    return ((2000 + int(parts[2])) * 100 + int(parts[0])) * 100 + int(parts[1])


def loadbyline(filename):
    result = []
    if not os.path.exists(filename):
        print('{0} not found'.format(filename))
        return result
    with open(filename, 'r') as f:
        for word in f.readlines():
            word = word.strip()
            result.append(word)
    return result


def loadcsv(filename, header):
    result = []
    if not os.path.exists(filename):
        print('{0} not found'.format(filename))
        return result
    with open(filename, 'r') as csvfile:
        r = csv.reader(csvfile)
        for each in r:
            result.append(each)
    if header:
        return result[1:], result[0]
    else:
        return result, None


def dump(path, filename, lines, mode='w'):
    if not os.path.exists(path):
        os.makedirs(path)
    if not path.endswith('/'):
        path += '/'
    with open(path+filename, mode) as f:
        map(f.write, lines)


def dumpcsv(path, filename, rows, header=None, mode='w'):
    if not os.path.exists(path):
        os.makedirs(path)
    if not path.endswith('/'):
        path += '/'
    with open(path+filename, mode) as f:
        w = csv.writer(f)
        if header:
            w.writerow(header)
        map(w.writerow, rows)


def parsefilename(filepath):
    filename = filepath if '/' not in filepath else filepath.split('/')[-1]
    filename = filename if '.' not in filename else filename.split('.')[0]
    return filename