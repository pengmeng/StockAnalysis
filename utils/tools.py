__author__ = 'mengpeng'
import os
import time


def gethash(string, cap=0xffffffff):
    return hash(string) & cap


def date2num(date):
    """Parse date format 01/17/14 into int 20140117"""
    parts = date.split('/')
    if len(parts) != 3:
        parts = time.strftime("%m/%d/%y", time.localtime()).split('/')
    return ((2000 + int(parts[2])) * 100 + int(parts[0])) * 100 + int(parts[1])


def loadkeywords(filename):
    result = []
    if not os.path.exists(filename):
        print('{0} not found'.format(filename))
        return result
    with open(filename, 'r') as f:
        for word in f.readlines():
            word = word.strip()
            result.append(word)
    return result


def dump(path, filename, lines, mode='w'):
    if not os.path.exists(path):
        os.makedirs(path)
    with open(path+filename, mode) as f:
        map(f.write, lines)