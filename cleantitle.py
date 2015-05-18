__author__ = 'mengpeng'
import time
from mongojuice.document import Document


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
    with open(filename, 'r') as f:
        for word in f.readlines():
            word = word.strip()
            result.append(word)
    return result


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


def main():
    words = loadkeywords('./data/word.txt')
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


if __name__ == '__main__':
    main()