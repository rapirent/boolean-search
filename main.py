# -*- coding: utf-8 -*-

import sys
import csv
import re

RE_CJK = re.compile(r'[\u4e00-\ufaff]+', re.UNICODE)
RE_ENG = re.compile(r'[a-zA-Z]+')
RE_ALL = re.compile(r'[a-zA-Z\u4e00-\ufaff]+', re.UNICODE)


class Trie():
    pass

def split_line(filename):
    csvfile = open(filename, 'r', newline='')
    sourcereader = csv.reader(csvfile, delimiter=',')
    index = list()
    for line in sourcereader:
        cjk_strings = RE_CJK.findall(line[1])
        eng_strings = RE_ENG.findall(line[1])
        split_string = set()
        for string in cjk_strings:
            split_string.update([string[i:i+2] for i in range(0, len(string))])
            split_string.update([string[i:i+3] for i in range(0, len(string))])
        if eng_strings:
            split_string.update(eng_strings)
        index.append(split_string)
    csvfile.close()

    return index



if __name__ == '__main__':

    import argparse
    # python main.py --source source.csv --query query.txt --output output.txt
    parser = argparse.ArgumentParser()
    parser.add_argument('--source',
                        default='source.csv',
                        help='input source data file name')
    parser.add_argument('--query',
                        default='query.txt',
                        help='query file name')
    parser.add_argument('--output',
                        default='output.txt',
                        help='output file name')
    args = parser.parse_args()

    index_string = split_line(args.source)

    with open(args.output, 'w') as o, open(args.query, 'r') as q:
        for query_line in q:
            query_line = re.sub('\n', '', query_line)
            print_list = list()
            if 'or' in query_line:
                queries = set(re.split(' or ', query_line))
                for (i, search_line) in enumerate(index_string):
                    if queries & search_line:
                        print_list.append(i+1)
            elif 'and' in query_line:
                queries = set(re.split(' and ',query_line))
                for (i, search_line) in enumerate(index_string):
                    if queries < search_line:
                        print_list.append(i+1)

            elif 'not' in query_line:
                queries = re.split(' not ', query_line)
                in_element = queries[0]
                notin_element = set(queries[1:])
                for (i, search_line) in enumerate(index_string):
                    if (in_element in search_line
                        and not notin_element < search_line):
                        print_list.append(i+1)

            if not print_list:
                print('0', file=o)
            else:
                print(','.join(map(str, print_list)), file=o)
