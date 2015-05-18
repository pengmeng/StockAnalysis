__author__ = 'mengpeng'
import argparse
import os
import pymongo


def buildpipeline(keyword):
    return [{'$match': {'keyword': keyword, 'datenum': {'$ne': 0}}},
            {'$group': {'_id': '$datenum', 'total': {'$sum': 1}}},
            {'$sort': {'_id': 1}}]


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


def dump(resultpath, filename, lines):
    if not os.path.exists(resultpath):
        os.makedirs(resultpath)
    with open(resultpath+filename, 'w') as f:
        map(f.write, lines)


def main(filename, resultpath):
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

if __name__ == '__main__':
    result_path = './data/keyword/'
    argparser = argparse.ArgumentParser()
    argparser.add_argument('keywordfile', type=str,
                           help="Keyword file that will be loaded")
    argparser.add_argument('-p', '--path', type=str, default=result_path,
                           help='Path to store the data (default: {0})'.format(result_path))
    args = argparser.parse_args()
    main(args.keywordfile, args.path)