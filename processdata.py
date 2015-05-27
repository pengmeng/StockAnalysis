__author__ = 'mengpeng'
import argparse
import pymongo
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
        return '_id: {0}\ntitle: {1}\ndate: {2}\ntag: {3}\nkeyword: {4}'\
            .format(self._id, self.title, self.date, self.tag, self.keyword)


def buildpipeline(keyword):
    return [{'$match': {'keyword': keyword, 'datenum': {'$ne': 0}}},
            {'$group': {'_id': '$datenum', 'total': {'$sum': 1}}},
            {'$sort': {'_id': 1}}]


def countkeyword(filename, resultpath):
    client = pymongo.MongoClient('localhost', 27017)
    mongo = client['pycrawler']['wsj']
    keywords = loadkeywords(filename)
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
    words = loadkeywords(filename)
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


def extracttitle(filename, resultpath, limit):
    words = loadkeywords(filename)
    outfile = filename if '/' not in filename else filename.split('/')[-1]
    outfile = outfile if '.' not in outfile else outfile.split('.')[0]
    for word in words:
        query = {'datenum': {'$gte': 20050101, '$lte': 20150430}, 'keyword': word}
        articles = Article.find(Article, query, limit=limit)
        print('Found {0} results for {1}'.format(len(articles), word))
        temp = []
        for each in iter(articles):
            temp.append(each.title+'\n')
        dump(resultpath, outfile, temp, 'a')
        del articles
    print('Done.')


def main(option, keywordfile, resultpath, limit):
    if option == 'count':
        countkeyword(keywordfile, resultpath)
    elif option == 'clean':
        cleantitle(keywordfile)
    elif option == 'extract':
        extracttitle(keywordfile, resultpath, limit)


if __name__ == '__main__':
    result_path = './data/keyword/'
    argparser = argparse.ArgumentParser()
    argparser.add_argument('option', type=str,
                           choices=['count', 'clean', 'extract'],
                           help='Option to perform')
    argparser.add_argument('keywordfile', type=str,
                           help="Keyword file that will be loaded")
    argparser.add_argument('-p', '--path', type=str, default=result_path,
                           help='Path to store the data (default: {0})'.format(result_path))
    argparser.add_argument('-l', '--limit', type=int, default=10,
                           help='Limit when query the database (default: 10)')
    args = argparser.parse_args()
    main(args.option, args.keywordfile, args.path, args.limit)