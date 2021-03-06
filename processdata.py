__author__ = 'mengpeng'
import argparse
import pymongo
from collections import OrderedDict
from utils.tools import *
from mongojuice.document import Document


@Document.register
class Article(Document):
    structure = {'_id': int,
                 'title': str,
                 'url': str,
                 'date': str,
                 'datenum': int,
                 'tag': str,
                 'keyword': str}
    given = ['title', 'url', 'date', 'tag', 'keyword']
    database = 'pycrawler'
    collection = 'wsj'

    def __init__(self, title, url, date, tag, keyword):
        super(Article, self).__init__()
        self.title = title
        self.url = url
        self.date = self._cleandate(date)
        self.datenum = date2num(self.date)
        self.tag = tag
        self.keyword = keyword
        self._id = gethash(title + self.date + keyword)

    def _cleandate(self, date):
        if date.count('/') == 2:
            return date
        else:
            return time.strftime("%m/%d/%y", time.localtime())

    def __str__(self):
        return '_id: {0}\ntitle: {1}\ndate: {2}\ntag: {3}\nkeyword: {4}' \
            .format(self._id, self.title, self.date, self.tag, self.keyword)


def buildpipeline(keyword):
    return [{'$match': {'keyword': keyword, 'datenum': {'$ne': 0}}},
            {'$group': {'_id': '$datenum', 'total': {'$sum': 1}}},
            {'$sort': {'_id': 1}}]


def countkeyword(filename, resultpath):
    client = pymongo.MongoClient('localhost', 27017)
    mongo = client['pycrawler']['wsj']
    keywords = loadbyline(filename)
    for each in keywords:
        print('Processing '+each)
        cursor = mongo.aggregate(buildpipeline(each))
        if cursor:
            cursor.next()
            result = map(lambda x: str(x['_id'])+','+str(x['total'])+'\n', cursor)
            cursor.close()
            print('{0} results.'.format(len(result)))
            dump(resultpath, each, result)
        print(each+' finished')
    client.close()


def cleantitle(filename):
    words = loadbyline(filename)
    for word in words:
        query = {'datenum': {'$gte': 20050101, '$lte': 20150430}, 'keyword': word}
        articles = Article.find(Article, query)
        print('Found {0} results for {1}'.format(len(articles), word))
        count = 0
        for each in iter(articles):
            if '</strong>' in each.title:
                each.title = each.title.replace('<strong class="highlight">', '')
                each.title = each.title.replace('</strong>', '')
                each.update()
                count += 1
        del articles
        print('Clean {0} items.'.format(count))
    print('Done.')


def extracttitle(filename, resultpath, start, end, limit):
    words = loadbyline(filename)
    outfile = parsefilename(filename)
    for word in words:
        query = {'datenum': {'$gte': start, '$lte': end}, 'keyword': word}
        articles = Article.find(Article, query, limit=limit, sort=[('datenum', 1)])
        print('Found {0} results for {1}'.format(len(articles), word))
        result = []
        for each in iter(articles):
            title = each.title.replace(',', '')
            result.append([each.datenum, title])
        dumpcsv(resultpath, outfile, result)
        del articles
    print('Done.')


def jointwofile(firstfile, withfile, header, resultpath):
    first, _ = loadcsv(firstfile, header)
    second, _ = loadcsv(withfile, header)
    match = {}
    for each in second:
        match[each[0]] = each[1]
    del second
    for i, each in enumerate(first):
        if each[0] in match:
            t = match[each[0]]
        else:
            t = '0'
        each.append(t)
        first[i] = each
    outfile = parsefilename(firstfile) + '-join-' + parsefilename(withfile) + '.csv'
    dumpcsv(resultpath, outfile, first)


def aggregate(inputfile, header, resultpath, outfile):
    lines, _ = loadcsv(inputfile, header)
    outfile = outfile if outfile else parsefilename(inputfile)
    acc = OrderedDict()
    for line in iter(lines):
        if line[0] in acc:
            acc[line[0]] += 1 if line[-1] == 'pos' else -1
        else:
            acc[line[0]] = 0
    result = [[k, v] for k, v in acc.iteritems()]
    dumpcsv(resultpath, outfile, result, ['date', 'count'])


def main(option, keywordfile, resultpath, limit, withfile, header, start, end, output):
    if option == 'count':
        countkeyword(keywordfile, resultpath)
    elif option == 'clean':
        cleantitle(keywordfile)
    elif option == 'extract':
        extracttitle(keywordfile, resultpath, start, end, limit)
    elif option == 'join':
        jointwofile(keywordfile, withfile, header, resultpath)
    elif option == 'aggregate':
        aggregate(keywordfile, header, resultpath, output)


if __name__ == '__main__':
    result_path = './data/'
    argparser = argparse.ArgumentParser()
    argparser.add_argument('option', type=str,
                           choices=['count', 'clean', 'extract', 'join', 'aggregate'],
                           help='Option to perform')
    argparser.add_argument('inputfile', type=str,
                           help="Input file to be processed")
    argparser.add_argument('-p', '--path', type=str, default=result_path,
                           help='Path to store the data (default: {0})'.format(result_path))
    argparser.add_argument('-o', '--output', type=str,
                           help='output file name')
    argparser.add_argument('-l', '--limit', type=int, default=0,
                           help='Limit when query the database (default: 10)')
    argparser.add_argument('-w', '--withfile', type=str,
                           help='File to join the inputfile')
    argparser.add_argument('--header', action="store_true",
                           help='Indicate that csv file has header line')
    argparser.add_argument('-s', '--shift', type=int, default=0,
                           help='Shift # of days among diffrent data')
    argparser.add_argument('--start', type=int, default=20050101,
                           help='start date (default: 20050101)')
    argparser.add_argument('--end', type=int, default=20150430,
                           help='end date (default: 20150430)')
    args = argparser.parse_args()
    main(args.option, args.inputfile, args.path, args.limit, args.withfile, args.header, args.start, args.end,
         args.output)