__author__ = 'mengpeng'
import argparse
from utils.tools import *
from textblob import TextBlob
from textblob.sentiments import NaiveBayesAnalyzer


def bypattern(titles, resultpath, outfile):
    result = []
    for each in titles:
        blob = TextBlob(each[1])
        temp = each + [blob.sentiment.polarity, blob.sentiment.subjectivity]
        if blob.sentiment.polarity > 0:
            temp.append('pos')
        elif blob.sentiment.polarity < 0:
            temp.append('neg')
        else:
            temp.append('neu')
        result.append(temp)
    dumpcsv(resultpath,
            outfile+'-pattern.csv',
            result,
            header=['date', 'title', 'polarity', 'subjectivity', 'classification'])


def bybayes(titles, resultpath, outfile):
    result = []
    for each in titles:
        blob = TextBlob(each[1], analyzer=NaiveBayesAnalyzer())
        result.append(each + [blob.sentiment.p_pos, blob.sentiment.p_neg, blob.sentiment.classification])
    dumpcsv(resultpath, outfile+'-bayes.csv', result, header=['date', 'title', 'p_pos', 'p_neg', 'classification'])


def main(filename, resultpath, output, classifier, header):
    outfile = output if output else parsefilename(filename)
    titles, _ = loadcsv(filename, header)
    if classifier == 'pattern':
        bypattern(titles, resultpath, outfile)
    elif classifier == 'bayes':
        bybayes(titles, resultpath, outfile)


if __name__ == '__main__':
    result_path = './data/sentiment/'
    argparser = argparse.ArgumentParser()
    argparser.add_argument('titlefile', type=str,
                           help="Title file that will be loaded")
    argparser.add_argument('-p', '--path', type=str, default=result_path,
                           help='Path to store the data (default: {0})'.format(result_path))
    argparser.add_argument('-o', '--output', type=str,
                           help='Output file name')
    argparser.add_argument('-c', '--classifier', type=str, default='pattern', choices=['pattern', 'bayes'],
                           help='Classifier (default: pattern)')
    argparser.add_argument('--header', help='Indicate that csv file has header line')
    args = argparser.parse_args()
    main(args.titlefile, args.path, args.output, args.classifier, args.header)