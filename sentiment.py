__author__ = 'mengpeng'
import argparse
from utils.tools import loadbyline
from utils.tools import dump
from textblob import TextBlob
from textblob.sentiments import NaiveBayesAnalyzer


def bypattern(titles, resultpath, outfile):
    result = ['title,polarity,subjectivity,classification\n']
    for each in titles:
        blob = TextBlob(each)
        s = '{0},{1},{2},'.format(each, blob.sentiment.polarity, blob.sentiment.subjectivity)
        if blob.sentiment.subjectivity >= 0.5:
            if blob.sentiment.polarity > 0:
                s += 'pos'
            elif blob.sentiment.polarity < 0:
                s += 'neg'
            else:
                s += 'neu'
        else:
            s += 'neu'
        s += '\n'
        result.append(s)
        dump(resultpath, outfile+'-pattern.csv', result)


def bybayes(titles, resultpath, outfile):
    result = ['title,p_pos,p_neg,classification\n']
    for each in titles:
        blob = TextBlob(each, analyzer=NaiveBayesAnalyzer())
        s = '{0},{1},{2},{3}\n'.format(each,
                                       blob.sentiment.p_pos,
                                       blob.sentiment.p_neg,
                                       blob.sentiment.classification)
        result.append(s)
    dump(resultpath, outfile+'-bayes.csv', result)


def main(filename, resultpath, classifier):
    outfile = filename if '/' not in filename else filename.split('/')[-1]
    outfile = outfile if '.' not in outfile else outfile.split('.')[0]
    titles = loadbyline(filename)
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
    argparser.add_argument('-c', '--classifier', type=str, default='pattern', choices=['pattern', 'bayes'],
                           help='Path to store the data (default: pattern)')
    args = argparser.parse_args()
    main(args.titlefile, args.path, args.classifier)